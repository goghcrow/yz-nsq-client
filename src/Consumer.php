<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Components\Nsq\Utils\Backoff;
use Zan\Framework\Foundation\Core\Debug;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;
use Zan\Framework\Utilities\Types\Time;


/**
 * Class Consumer
 * @package Zan\Framework\Components\Nsq
 *
 * Consumer : Connection = 1 : N (one nsqd one connection)
 */
class Consumer implements ConnDelegate
{
    const BackoffSignal = 0;
    const ContinueSignal = 1;
    const ResumeSignal = 2;

    // 同时接受消息的最大数量
    private $maxInFlight; // limit

    // 该消费者当前总的RDY数量
    // 当某连接断开,从总量移除
    // 当收到消息,从总量-1
    // 当某连接发送新RDY, 则总量加上(新RDY - RDY存量) : $this->totalRdyCount += $count - $conn->getRDY();
    private $totalRdyCount = 0;

    // TODO 为什么需要两个
    // 当前backoff持续时间, backoff之后尝试恢复
    private $backoffDuration;
    private $backoffCounter; // backoff次数,attempts

    // connected to nsqlookupd or nsqd, prevent add msghandler after connected
    private $isConnected;
    private $isStopped;

    private $topic;
    private $channel;

    /**
     * 是否需要在多个连接间重新分配RDY
     * @var bool
     */
	private $needRDYRedistributed = false;

    /**
     * 未完成nsqd连接列表
     * @var Connection[] map<string, Connection> [ "host:port" => conn ]
     */
    private $pendingConnections = [];

    /**
     * nsqd连接列表
     * @var Connection[] map<string, Connection> [ "host:port" => conn ]
     */
    private $connections = [];

    /**
     * nsqd地址列表
     * @var string[] set [ "host:port" => true, ]
     */
	private $nsqdTCPAddrs = [];

    /**
     * NSQLookup地址列表
     * @var string[] [ "http://nsq-dev.s.qima-inc.com:4161", ...... ]
     */
	private $lookupdHTTPAddrs = [];
	private $lookupdQueryIndex = 0;

    /**
     * RDY状态更新重试计时器列表
     * @var array map<string, Timer>  [timerId => timer]
     */
    private $rdyRetryTimers = [];

    /**
     * @var MsgHandler
     */
    private $msgHandler;

    /**
     * 消息统计数据
     * @var array
     */
    private $stats;

    /**
     * Consumer constructor.
     * @param string $topic
     * @param string $channel
     * @param MsgHandler $msgHandler
     */
    public function __construct($topic, $channel, MsgHandler $msgHandler)
    {
        $this->topic = Command::checkTopicChannelName($topic);
        $this->channel = Command::checkTopicChannelName($channel);
        $this->maxInFlight = NsqConfig::getMaxInFlightCount();
        $this->msgHandler = $msgHandler;
        $this->stats = [
            "messagesReceived" => 0,
            "messagesFinished" => 0,
            "messagesRequeued" => 0,
        ];

        $this->bootRedistributeRDYTick();
    }

    /**
     * 统计信息
     * @return array
     */
    public function stats()
    {
        return $this->stats + ["connections" => count($this->connections)];
    }

    /**
     * 消费者实例的某个连接是否阻塞()而不能接受新消息
     * 比如RDY=0且连接未close
     * 阻塞: 上次RDY数量的85%以上都在处理中
     *
     * @return bool
     */
    public function isStarved()
    {
        /* @var Connection $conn */
        foreach ($this->connections as $conn) {
            $threshold = intval($conn->lastRDY() * 0.85);
            $inFlight = $conn->getMsgInFlight();

            if ($inFlight >= $threshold && $inFlight > 0 && !$conn->isClosing()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 设置当前消费者实例(多个NSQD连接)最大允许的in-flight消息数量,
     * 如果已连接, 则将在所有连接间重新分配额度, 更新RDY状态
     * 例如, ChangeMaxInFlight(0) 将暂停所有消息处理
     *
     * @param int $maxInFlight
     */
    public function changeMaxInFlight($maxInFlight)
    {
        if ($this->maxInFlight - $maxInFlight === 0) {
            return;
        }

        $this->maxInFlight = max(0, intval($maxInFlight));
        foreach ($this->connections as $conn) {
            $this->maybeUpdateRDY($conn);
        }
    }

    /**
     * 连接NSQLookupd, 并且把改nsq地址加入消费者实例地址列表
     * 如果是加入的第一个lookupd地址
     *  则发起http请求发现topic的nsqd地址
     *  启动tick定期是持续更新nsqd地址变化
     *
     * @param string $address
     * @return \Generator|void
     * @throws NsqException
     */
    public function connectToNSQLookupd($address)
    {
        if ($this->isStopped) {
            throw new NsqException("consumer stopped");
        }
        $address = $this->formatAddress($address);

        $this->isConnected = true;

        if (in_array($address, $this->lookupdHTTPAddrs, true)) {
            yield true;
            return;
        }
        $this->lookupdHTTPAddrs[] = $address;

        if (count($this->lookupdHTTPAddrs) === 1) {
            yield $this->queryLookupd();
            $this->lookupdPolling();
        }
    }

    /**
     * 支持多个NSQLookup地址
     * @param array $addresses
     * @return \Generator
     */
    public function connectToNSQLookupds(array $addresses)
    {
        foreach ($addresses as $address) {
            yield $this->connectToNSQLookupd($address);
        }
    }

    private function formatAddress($addr)
    {
        $scheme = parse_url($addr, PHP_URL_SCHEME);
        $host = parse_url($addr, PHP_URL_HOST);
        $port = parse_url($addr, PHP_URL_PORT) ?: 80;
        if (!$scheme || !$host) {
            throw new NsqException("invalid address $addr");
        }
        $host = strtolower($host);
        return "$scheme://$host:$port";
    }

    /**
     * 从周期性检查列表中移除某个NSQLookupd地址
     *
     * @param string $addr
     * @return bool
     * @throws NsqException
     */
    public function disconnectFromNSQLookupd($addr)
    {
        if (!in_array($addr, $this->lookupdHTTPAddrs, true)) {
            throw new NsqException("not connected");
        }

        if (count($this->lookupdHTTPAddrs) === 1) {
            throw new NsqException("cannot disconnect from only remaining nsqlookupd HTTP address $addr");
        }

        foreach ($this->lookupdHTTPAddrs as $i => $addr_) {
            if ($addr === $addr_) {
                unset($this->lookupdHTTPAddrs[$i]);
                return true;
            }
        }
        return false;
    }

    /**
     * 定时轮询所有nsq lookup服务器
     * @return void
     */
    private function lookupdPolling()
    {
        $this->bootLookupdPolling(function() {
            try {
                Task::execute($this->queryLookupd());
            } catch (\Exception $ex) {
                sys_echo("lookupdPolling exception {$ex->getMessage()}");
            }
        });
    }

    private function bootLookupdPolling(callable $callback)
    {
        // add some jitter
        $rand = (float)rand() / (float)getrandmax();
        $jitter = $rand * NsqConfig::getLookupdPollJitter() * NsqConfig::getLookupdPollInterval();
        Timer::after(intval($jitter), function() use($callback) {
            if ($this->isStopped) {
                return;
            }
            $interval = NsqConfig::getLookupdPollInterval();
            Timer::tick($interval, $callback, $this->lookupdPollingTickId());
        });
    }

    /**
     * Round-Robin轮询方式获取一个lookupd地址,
     * http请求获取订阅topic的所有nsqd地址, 连接所有nsqd地址并订阅
     * @return mixed
     */
    private function queryLookupd()
    {
        $nsqdList = [];

        $endpoint = $this->nextLookupdEndpoint();
        try {
            $lookup = new Lookup($endpoint);
            $nsqdList = (yield $lookup->queryNsqd($this->topic));
        } catch (\Exception $ex) {
            sys_echo("($endpoint) error query nsqd - {$ex->getMessage()}");
        }
        foreach ($nsqdList as list($host, $port)) {
            try {
                yield $this->connectToNSQD($host, $port);
                // Task::execute($this->connectToNSQD($host, $port)); // 并发连接
            } catch (\Exception $ex) {
                sys_echo("($host:$port) error connecting to nsqd - {$ex->getMessage()}");
            }
        }
        return;
    }

    /**
     * 记录当前使用并返回下一个lookup地址
     * (i + 1) mod n
     * @return string
     */
    private function nextLookupdEndpoint()
    {
        $num = count($this->lookupdHTTPAddrs);
        if ($this->lookupdQueryIndex >= $num) {
            $this->lookupdQueryIndex = 0;
        }
        $address = $this->lookupdHTTPAddrs[$this->lookupdQueryIndex];
        $this->lookupdQueryIndex = ($this->lookupdQueryIndex + 1) % $num;
        return $address;
    }

    /**
     * 直连NSQD实例
     * 建议连接NSQLookupd, 自动发现topic对应的nsqd实例
     * @param string $host
     * @param int $port
     * @return mixed
     * @throws \Exception
     */
    public function connectToNSQD($host, $port)
    {
        if ($this->isStopped) {
            throw new NsqException("consumer stopped");
        }
        $this->isConnected = true;

        $conn = new Connection($host, $port, $this);
        $addr = $conn->getAddr();
        if (isset($this->pendingConnections[$addr]) || isset($this->connections[$addr])) {
            throw new NsqException("$addr already connected");
        }

        $this->pendingConnections[$addr] = $conn;
        $this->nsqdTCPAddrs[$addr] = true;

        try {
            yield $conn->connect();
            $conn->writeCmd(Command::subscribe($this->topic, $this->channel));
            $this->connections[$addr] = $conn;
        } finally {
            unset($this->pendingConnections[$addr]);
        }

        // 加入新连接重新调整所有连接rdy状态
        foreach ($this->connections as $conn) {
            $this->maybeUpdateRDY($conn);
        }
        return;
    }

    /**
     * 断开NSQD连接
     * @param string $host
     * @param int $port
     * @return bool
     * @throws NsqException
     */
    public function disconnectFromNSQD($host, $port)
    {
        $key = "$host:$port";
        if (!isset($this->nsqdTCPAddrs[$key])) {
            throw new NsqException("not connected");
        }

        unset($this->nsqdTCPAddrs[$key]);
        if (isset($this->connections[$key])) {
            return $this->connections[$key]->tryClose();
        } else if (isset($this->pendingConnections[$key])) {
            return $this->pendingConnections[$key]->tryClose();
        }

        return false;
    }

    private function startStopContinueBackoff(Connection $conn, $signal)
    {
        // 避免大量成功或失败的结果, 保证backoff期间不连续增减backoffCounter
        if ($this->inBackoffTimeout()) {
            return;
        }

        // update backoff state
        $backoffUpdated = false;
        switch ($signal) {
            case static::ResumeSignal:
                if ($this->backoffCounter > 0) {
                    $this->backoffCounter--;
                    $backoffUpdated = true;
                }
                break;

            case static::BackoffSignal:
                // TODO backoff时间可能需要修改
                $nextBackoff = $this->getNextBackoff($this->backoffCounter + 1);
                if ($nextBackoff <= NsqConfig::getMaxBackoffDuration()) {
                    $this->backoffCounter++;
                    $backoffUpdated = true;
                }
                break;
        }

        if ($this->backoffCounter === 0 && $backoffUpdated)
        {
            // 离开backoff状态
            $count = $this->perConnMaxInFlight();
            sys_echo("exiting backoff, returning all to RDY $count");

            foreach ($this->connections as $conn) {
                $this->updateRDY($conn, $count);
            }
        } else if ($this->backoffCounter > 0) {
            // 开始或继续back状态
            $backoffDuration = $this->getNextBackoff($this->backoffCounter);
            $backoffDuration = min(NsqConfig::getMaxBackoffDuration(), $backoffDuration);
            sys_echo("backing off for ${backoffDuration}ms (backoff level {$this->backoffCounter}), setting all to RDY 0");

            foreach ($this->connections as $conn) {
                $this->updateRDY($conn, 0);
            }
            $this->backoff($backoffDuration);
        }
    }

    private function getNextBackoff($attempts)
    {
        $c = NsqConfig::getMessageBackoff();
        return Backoff::calculate($attempts,
            $c["min"], $c["max"], $c["factor"], $c["jitter"]);
    }

    private function backoff($delay)
    {
        $this->backoffDuration = $delay;
        Timer::after($delay, $this->resume());
    }

    private function resume()
    {
        return function() {
            if ($this->isStopped) {
                $this->backoffDuration = 0;
                return;
            }

            // pick a random connection to test the waters
            $connCount = count($this->connections);
            if ($connCount === 0) {
                $this->backoff(1000);
                return;
            }

            $choice = $this->connections[array_rand($this->connections)];

            // while in backoff only ever let 1 message at a time through
            try {
                $this->updateRDY($choice, 1);
            } catch (\Exception $ex) {
                sys_echo("({$choice->getAddr()}) error resuming RDY 1 - {$ex->getMessage()}");
                sys_echo("backing off for 1 seconds");
                $this->backoff(1000);
                return;
            }

            $this->backoffDuration = 0;
        };
    }

    /**
     * 是否backoff状态
     * @return bool
     */
    private function inBackoff()
    {
        return $this->backoffCounter > 0;
    }

    /**
     * @return bool
     */
    private function inBackoffTimeout()
    {
        return $this->backoffDuration > 0;
    }

    /**
     * 当连接 RDY余量<=1 或 低于25% 或 消费者连接数改变,
     * 导致当前RDY在不同连接直接分配出现不均衡, 则更连接RDY
     * @param Connection $conn
     */
    private function maybeUpdateRDY(Connection $conn)
    {
        if ($this->inBackoff() || $this->inBackoffTimeout()) {
            return;
        }

        $remainRDY = $conn->getRemainRDY();
        $lastRdyCount = $conn->lastRDY();
        $perConnMax = $this->perConnMaxInFlight();

        if ($remainRDY <= 1 || $remainRDY < ($lastRdyCount / 4) || ($perConnMax > 0 && $perConnMax < $remainRDY)) {
            $this->updateRDY($conn, $perConnMax);
        }
    }

    /**
     * 计算每条连接的max-in-flight数量
     * 该值会随着连接数动态变化
     * @return int
     */
    private function perConnMaxInFlight()
    {
        $max = $this->maxInFlight;
        $per = $this->maxInFlight / count($this->connections);
        return intval(min(max(1, $per), $max));
    }

    private function bootRedistributeRDYTick()
    {
        $interval = NsqConfig::getRdyRedistributeInterval();
        Timer::tick($interval, $this->redistributeRDY(), $this->rdyLoopTickId());
    }

    private function redistributeRDY()
    {
        return function() {
            try {
                if ($this->inBackoffTimeout()) {
                    return;
                }

                $connNum = count($this->connections);
                if ($connNum === 0) {
                    return;
                }
                if ($connNum > $this->maxInFlight) {
                    $this->needRDYRedistributed = true;
                }
                if ($this->inBackoff() && $connNum > 1) {
                    $this->needRDYRedistributed = true;
                }

                $this->doRedistributeRDY();

            } catch (\Exception $ex) {
                sys_echo("redistributeRDY exception {$ex->getMessage()}");
            }
        };
    }

    private function doRedistributeRDY()
    {
        if ($this->needRDYRedistributed === false) {
            return;
        }

        $this->needRDYRedistributed = false;

        $possibleConns = [];
        foreach ($this->connections as $conn) {
            $lastMsgDuration = Time::stamp() - $conn->lastMessageTime();
            // 尚有未收到的rdy(可能消息已消费完) && 最后一次收到消息距离现在已超过n毫秒
            // 则该连接暂时暂停接受消息
            if ($conn->getRemainRDY() > 0 && $lastMsgDuration > NsqConfig::getLowRdyIdleTimeout()) {
                $this->updateRDY($conn, 0);
            }
            $possibleConns[] = $conn;
        }

        // 剩余inFlight额度
        $availableMaxInFlight = $this->maxInFlight - $this->totalRdyCount;
        if ($this->inBackoff()) {
            $availableMaxInFlight = 1 - $this->totalRdyCount;
        }

        while ($possibleConns && $availableMaxInFlight > 0) {
            $availableMaxInFlight--;

            $i = array_rand($possibleConns);
            $conn = $possibleConns[$i];
            try {
                $this->updateRDY($conn, 1);
            } catch (\Exception $ex) {
                sys_echo("nsq update rdy exception: {$ex->getMessage()}");
            }
            unset($possibleConns[$i]);
        }
    }

    /**
     * @param Connection $conn
     * @param int $count
     * @throws NsqException
     */
    private function updateRDY(Connection $conn, $count)
    {
        if ($conn->isClosing()) {
            throw new NsqException("already closed");
        }

        $count = min($count, $conn->maxRDY());
        $timerId = $this->rdyRetryAfterId($conn);
        if (isset($this->rdyRetryTimers[$timerId])) {
            // 清除未决的RDY状态更新计时器
            Timer::clearAfterJob($timerId);
        }

        // 永远不超过customer的max-in-flight限制, 超过则truncate掉, 新连接可以获取部分max-in-flight
        $remainRdy = $conn->getRemainRDY();
        // 当前连接允许设置的最大RDY值 = 连接原有RDY + (允许最大值 - 当前customer总RDY) = 连接原有RDY + 余量
        $maxPossibleRdy = $remainRdy + ($this->maxInFlight - $this->totalRdyCount);
        if ($maxPossibleRdy > 0 && $maxPossibleRdy < $count) {
            $count = $maxPossibleRdy;
        }

        // todo why < 0
        if ($maxPossibleRdy <= 0 && $count > 0) {
            if ($remainRdy === 0) {
                // 我们想要离开zeroRDY状态,但是不能
                // 为了避免永远饥饿的状态, 5s后重新调度
                // 任意的RDY状态更新成功, 定时器会被取消
                Timer::after(5000, function() use($conn, $count) {
                    try {
                        $this->updateRDY($conn, $count);
                    } catch (\Exception $ex) {
                        sys_echo("update rdy exception: {$ex->getMessage()}");
                    }
                }, $timerId);

                $this->rdyRetryTimers[$timerId] = true;
            }
        }

        return $this->sendRDY($conn, $count);
    }

    private function sendRDY(Connection $conn, $count)
    {
        if ($count === 0 && $conn->lastRDY() === 0) {
            return;
        }

        // 新设置的RDY减去上次剩余的RDY额度
        $this->totalRdyCount += $count - $conn->getRemainRDY();

        $conn->setRDY($count);
        $conn->writeCmd(Command::ready($count));
    }

    public function stop()
    {
        $this->isStopped = true;
        if (count($this->connections) === 0) {
            return;
        }

        foreach ($this->connections as $conn) {
            try {
                $conn->writeCmd(Command::startClose());
            } catch (\Exception $ex) {
                sys_echo(__METHOD__ . " ex: " . $ex->getMessage());
            }
        }

        Timer::after(5000, function() {
            // stop lookupdPolling and rdyLoop
            Timer::clearTickJob($this->lookupdPollingTickId());
            Timer::clearTickJob($this->rdyLoopTickId());
        });
    }

    private function shouldFailMessage(Message $msg)
    {
        $maxAttempts = NsqConfig::getMaxAttempts();
        $attempts = $msg->getAttempts();
        if ($maxAttempts > 0 && $attempts > $maxAttempts) {
            sys_echo("msg {$msg->getId()} attempted {$attempts} times, giving up");
            $this->msgHandler->logFailedMessage($msg);
            return true;
        }

        return false;
    }

    private function handleMessage(Message $msg)
    {
        if ($this->shouldFailMessage($msg)) {
            $msg->finish();
            return;
        }

        try {
            $success = (yield $this->msgHandler->handleMessage($msg));
            if ($msg->isAutoResponseDisabled()) {
                return;
            }
            if ($success) {
                $msg->finish();
                return;
            }
        } catch (\Exception $ex) {
            sys_echo("consumer handle message fail({$ex->getMessage()}), requeue");

        }

        if (NsqConfig::enableBackoff()) {
            $msg->requeue(-1);
        } else {
            $msg->requeueWithoutBackoff(-1);
        }
    }

    /**
     * OnMessage is called when the connection
     * receives a FrameTypeMessage from nsqd
     * @param Connection $conn
     * @param Message $msg
     * @return void
     */
    public function onMessage(Connection $conn, Message $msg)
    {
        $this->totalRdyCount--;
        $this->stats["messagesReceived"]++;

        yield $this->handleMessage($msg);
        // 收到消息后更新rdy
        $this->maybeUpdateRDY($conn);
    }

    /**
     * OnMessageFinished is called when the connection
     * handles a FIN command from a message handler
     * @param Connection $conn
     * @param Message $msg
     * @return void
     */
    public function onMessageFinished(Connection $conn, Message $msg)
    {
        $this->stats["messagesFinished"]++;
    }

    /**
     * OnMessageRequeued is called when the connection
     * handles a REQ command from a message handler
     * @param Connection $conn
     * @param Message $msg
     * @return void
     */
    public function onMessageRequeued(Connection $conn, Message $msg)
    {
        $this->stats["messagesRequeued"]++;
    }

    /**
     * OnBackoff is called when the connection triggers a backoff state
     * @param Connection $conn
     * @return void
     */
    public function onBackoff(Connection $conn)
    {
        $this->startStopContinueBackoff($conn, static::BackoffSignal);
    }

    /**
     * OnContinue is called when the connection f
     * inishes a message without adjusting backoff state
     * @param Connection $conn
     * @return void
     */
    public function onContinue(Connection $conn)
    {
        $this->startStopContinueBackoff($conn, static::ContinueSignal);
    }

    /**
     * OnResume is called when the connection
     * triggers a resume state
     * @param Connection $conn
     * @return void
     */
    public function onResume(Connection $conn)
    {
        $this->startStopContinueBackoff($conn, static::ResumeSignal);
    }

    /**
     * OnResponse is called when the connection
     * receives a FrameTypeResponse from nsqd
     * @param Connection $conn
     * @param string $bytes
     * @return mixed
     */
    public function onResponse(Connection $conn, $bytes)
    {
        if ($bytes === "CLOSE_WAIT") {
            // nsqd ack客户端的StartClose, 准备关闭
            // 可以假定不会再收到任何nsqd的消息
            $conn->tryClose();
        }
        return;
    }

    /**
     * OnIOError is called when the connection experiences
     * a low-level TCP transport error
     * @param Connection $conn
     * @param \Exception $ex
     * @return void
     */
    public function onIOError(Connection $conn, \Exception $ex)
    {
        try {
            $conn->tryClose();
        } catch (\Exception $ignore) {}
    }

    /**
     * @param Connection $conn
     * @return void
     */
    public function onClose(Connection $conn)
    {
        $hasRDYRetryTimer = false;

        // 从消费者实例的总RDY数量减去当前连接的剩余RDY数量
        $remainRdyCount = $conn->getRemainRDY();
        $this->totalRdyCount -= $remainRdyCount;

        $timerId = $this->rdyRetryAfterId($conn);
        if (isset($this->rdyRetryTimers[$timerId])) {
            // 清除未决的RDY状态更新计时器
            Timer::clearAfterJob($timerId);
            unset($this->rdyRetryTimers[$timerId]);
            $hasRDYRetryTimer = true;
        }

        unset($this->connections[$conn->getAddr()]);

        $remainConnNum = count($this->connections);
        sys_echo("nsq consumer onClose: there are $remainConnNum connections left alive");

        // TODO
        // 该连接有未处理的RDY, 触发一个RDY分配保证当前退出连接的RDY转移到新连接
        if (($hasRDYRetryTimer || $remainRdyCount > 0) && ($remainConnNum === $this->maxInFlight || $this->inBackoff())
        ) {
            $this->needRDYRedistributed = true;
        }

        // 最后一个连接退出
        if ($this->isStopped && $remainConnNum === 0) {
            return;
        }

        // nsqd连接断开重新连接
        $this->reconnect($conn);
    }

    private function reconnect(Connection $conn)
    {
        $numLookupd = count($this->lookupdHTTPAddrs);
        $reconnect = isset($this->nsqdTCPAddrs[$conn->getAddr()]);

        if ($numLookupd > 0) {

            // 触发一次lookup查询, 因为nsqd连接断开, 可能该nsqd实例已经下线
            Task::execute($this->queryLookupd());

        } else if ($reconnect) {

            // 没有lookupd只有nsqd地址, 则尝试5s后重新连接
            Timer::after(5000, function() use($conn) {
                if ($this->isStopped) {
                    return;
                }
                $reconnect = isset($this->nsqdTCPAddrs[$conn->getAddr()]);
                if (!$reconnect) {
                    return;
                }

                try {
                    Task::execute($this->connectToNSQD($conn->getHost(), $conn->getPort()));
                } catch (\Exception $ex) {
                    sys_echo("({$conn->getAddr()}) error reconnecting to nsqd - " . $ex->getMessage());
                }
            });

        }
    }


    public function onHeartbeat(Connection $conn) {}
    public function onError(Connection $conn, $bytes) {}

    private function lookupdPollingTickId()
    {
        return spl_object_hash($this) . "_lookupd_polling_tick_id";
    }

    private function rdyLoopTickId()
    {
        return spl_object_hash($this) . "_rdy_tick_id";
    }

    private function rdyRetryAfterId(Connection $conn)
    {
        return spl_object_hash($conn) . "_rdy_retry_timer_id";
    }

    public function onReceive(Connection $conn, $bytes) {
        if (Debug::get()) {
             // sys_echo("nsq({$conn->getAddr()}) recv:" . str_replace("\n", "\\n", $bytes));
        }
    }

    public function onSend(Connection $conn, $bytes) {
        if (Debug::get()) {
            sys_echo("nsq({$conn->getAddr()}) send:" . str_replace("\n", "\\n", $bytes));
        }
    }
}