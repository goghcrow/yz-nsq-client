<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Components\Contract\Nsq\ConnDelegate;
use Zan\Framework\Components\Contract\Nsq\MsgHandler;
use Zan\Framework\Components\Contract\Nsq\NsqdDelegate;
use Zan\Framework\Components\Nsq\Utils\Backoff;
use Zan\Framework\Foundation\Core\Debug;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;
use Zan\Framework\Utilities\Types\Time;


class Consumer implements ConnDelegate, NsqdDelegate
{
    const BackoffSignal = 0;
    const ContinueSignal = 1;
    const ResumeSignal = 2;

    // 同时接受消息的最大数量
    // 每个nsqd连接均分
    private $maxInFlight; // limit

    // 该消费者当前总的RDY数量
    // 当某连接断开,从总量移除
    // 当收到消息,从总量-1
    // 当某连接发送新RDY, 则总量加上(新RDY - RDY存量) : $this->totalRdyCount += $count - $conn->getRDY();
    private $totalRdyCount = 0;

    // 当前backoff持续时间, backoff之后尝试恢复
    private $backoffDuration;
    private $backoffCounter; // backoff次数,attempts

    /**
     * 是否需要在多个连接间重新分配RDY
     * @var bool
     */
	private $needRDYRedistributed = false;

    /**
     * @var array map<string, Timer>  [timerId => timer]
     */
    private $rdyRetryTimers = [];

    /**
     * @var MsgHandler
     */
    private $msgHandler;

    /**
     * @var Lookup
     */
    private $lookup;

    private $topic;

    private $channel;

    private $stats;

    /**
     * Consumer constructor.
     * @param $topic
     * @param string $channel
     * @param MsgHandler $msgHandler
     */
    public function __construct($topic, $channel, MsgHandler $msgHandler)
    {
        $this->topic = Command::checkTopicChannelName($topic);
        $this->channel = Command::checkTopicChannelName($channel);
        $this->maxInFlight = NsqConfig::getMaxInFlightCount();
        $this->msgHandler = $msgHandler;

        $this->lookup = new Lookup($this->topic);
        $this->lookup->setNsqdDelegate($this);

        $this->stats = [
            "messagesReceived" => 0,
            "messagesFinished" => 0,
            "messagesRequeued" => 0,
        ];

        $this->bootRedistributeRDYTick();
    }

    /**
     * @param $address
     * @return \Generator Consumer
     */
    public function connectToNSQLookupd($address)
    {
        yield $this->lookup->connectToNSQLookupd($address);
        yield $this;
    }

    /**
     * @param array $addresses
     * @return \Generator Consumer
     */
    public function connectToNSQLookupds(array $addresses)
    {
        foreach ($addresses as $address) {
            yield $this->connectToNSQLookupd($address);
        }
        yield $this;
    }

    public function disconnectFromNSQLookupd($addr)
    {
        $this->lookup->disconnectFromNSQLookupd($addr);
    }

    public function connectToNSQD($host, $port)
    {
        yield $this->lookup->connectToNSQD($host, $port);
    }

    public function disconnectFromNSQD($host, $port)
    {
        $this->lookup->disconnectFromNSQD($host, $port);
    }

    private function getNsqdConns()
    {
        return $this->lookup->getNSQDConnections();
    }

    /**
     * 统计信息
     * @return array
     */
    public function stats()
    {
        return $this->stats + $this->lookup->stats() + ["isStarved" => $this->isStarved()];
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

        foreach ($this->getNsqdConns() as $conn) {
            $this->maybeUpdateRDY($conn);
        }
    }

    /**
     * 消费者实例的某个连接是否处于饥饿状态
     * 比如RDY=0且连接未close
     * 阻塞: 上次RDY数量的85%以上都在处理中
     *
     * @return bool
     */
    public function isStarved()
    {
        foreach ($this->getNsqdConns() as $conn) {
            $threshold = intval($conn->lastRDY() * 0.85);
            $inFlight = $conn->getMsgInFlight();

            if ($inFlight >= $threshold && $inFlight > 0 && !$conn->isClosing()) {
                return true;
            }
        }
        return false;
    }

    private function startStopContinueBackoff(/** @noinspection PhpUnusedParameterInspection */
        Connection $conn, $signal)
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

            foreach ($this->getNsqdConns() as $conn) {
                $this->updateRDY($conn, $count);
            }
        } else if ($this->backoffCounter > 0) {
            // 开始或继续back状态
            $backoffDuration = $this->getNextBackoff($this->backoffCounter);
            $backoffDuration = min(NsqConfig::getMaxBackoffDuration(), $backoffDuration);
            sys_echo("backing off for ${backoffDuration}ms (backoff level {$this->backoffCounter}), setting all to RDY 0");

            foreach ($this->getNsqdConns() as $conn) {
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
            if ($this->lookup->isStopped()) {
                $this->backoffDuration = 0;
                return;
            }

            // pick a random connection to test the waters
            $connCount = count($this->getNsqdConns());
            if ($connCount === 0) {
                $this->backoff(1000);
                return;
            }

            $choice = $this->getNsqdConns()[array_rand($this->getNsqdConns())];

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
        $connNum = count($this->getNsqdConns());
        if ($connNum === 0) {
            return 0;
        }
        $max = $this->maxInFlight;
        $per = $this->maxInFlight / $connNum;
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

                $connNum = count($this->getNsqdConns());
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
        foreach ($this->getNsqdConns() as $conn) {
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
            /** @noinspection PhpInconsistentReturnPointsInspection */
            return;
        }

        $count = min($count, $conn->maxRDY());
        $timerId = $this->rdyRetryAfterId($conn);
        if (isset($this->rdyRetryTimers[$timerId])) {
            // 清除未决的RDY状态更新计时器
            Timer::clearAfterJob($timerId);
        }

        // 永远不超过customer的max-in-flight限制, 超过则truncate掉, 让新连接可以获取部分max-in-flight
        $remainRdy = $conn->getRemainRDY();
        // 当前连接允许设置的最大RDY值 = 连接原有RDY + (允许最大值 - 当前customer总RDY) = 连接原有RDY + 余量
        $maxPossibleRdy = $remainRdy + ($this->maxInFlight - $this->totalRdyCount);
        if ($maxPossibleRdy > 0 && $maxPossibleRdy < $count) {
            $count = $maxPossibleRdy;
        }

        // 积压产生, 且服务器要发送的rdy全部发送完毕, 且要离开zeroRDY(count>0)状态
        if ($maxPossibleRdy <= 0 &&  $remainRdy === 0 && $count > 0) {
            // 避免永远饥饿的状态, 5s后重新调度
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
        $this->lookup->stop();
        Timer::clearTickJob($this->rdyLoopTickId());
        if (count($this->getNsqdConns()) === 0) {
            return;
        }

        Timer::after(NsqConfig::getDelayingCloseTime(), function() {
            foreach ($this->getNsqdConns() as $nsqdConn) {
                $nsqdConn->tryClose();
            }
        });
    }

    private function shouldFailMessage(Message $msg)
    {
        $maxAttempts = NsqConfig::getMaxAttempts();
        $attempts = $msg->getAttempts();
        if ($maxAttempts > 0 && $attempts > $maxAttempts) {
            sys_echo("msg {$msg->getId()} attempted {$attempts} times, giving up");
            $this->msgHandler->logFailedMessage($msg, $this);
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
            $result = (yield $this->msgHandler->handleMessage($msg, $this));
            if (!$msg->isAutoResponse()) {
                return;
            }
            if ($result !== false) {
                $msg->finish();
                return;
            }
        } catch (\Exception $ex) {
            sys_echo("consumer handle message fail({$ex->getMessage()}), requeue");
        }

        $msg->requeue(-1, false);
    }

    /**
     * onConnected is called when nsqd connects
     * @param Connection $conn
     * @return void
     */
    public function onConnect(Connection $conn)
    {
        // 加入新连接重新调整所有连接rdy状态
        $conn->setDelegate($this);
        $conn->writeCmd(Command::subscribe($this->topic, $this->channel));
        $this->maybeUpdateRDY($conn);
    }

    /**
     * @param Connection $conn
     * @return \Generator
     */
    private function reconnectToNSQD(Connection $conn)
    {
        try {
            yield $this->lookup->reconnect($conn);

            $conn->writeCmd(Command::subscribe($this->topic, $this->channel));

            foreach ($this->getNsqdConns() as $conn) {
                $this->maybeUpdateRDY($conn);
            }
        } catch (\Exception $ex) {
            sys_echo("nsq({$conn->getAddr()}) reconnectToNSQD exception: {$ex->getMessage()}");
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
        } catch (\Exception $ex) {
            sys_echo("nsq({$conn->getAddr()}) onIOError exception: {$ex->getMessage()}");
        }
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

        $this->lookup->removeConnection($conn);

        $remainConnNum = count($this->getNsqdConns());
        sys_echo("nsq consumer onClose: there are $remainConnNum connections left alive");

        // 该连接有未处理的RDY, 触发一个RDY分配保证当前退出连接的RDY转移到新连接
        if (($hasRDYRetryTimer || $remainRdyCount > 0)
            && ($remainConnNum === $this->maxInFlight || $this->inBackoff())
        ) {
            $this->needRDYRedistributed = true;
        }

        if ($this->lookup->isStopped()/* && $remainConnNum === 0*/) {
            return;
        }

        // nsqd连接断开重新连接
        Task::execute($this->reconnectToNSQD($conn));
    }

    public function onHeartbeat(Connection $conn) {}
    public function onError(Connection $conn, $bytes) {}

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
            sys_echo("nsq({$conn->getAddr()}) recv:" . str_replace("\n", "\\n", $bytes));
        }
    }

    public function onSend(Connection $conn, $bytes) {
        if (Debug::get()) {
            sys_echo("nsq({$conn->getAddr()}) send:" . str_replace("\n", "\\n", $bytes));
        }
    }
}