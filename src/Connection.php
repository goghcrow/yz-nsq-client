<?php

namespace Zan\Framework\Components\Nsq;

use swoole_client as SwooleClient;
use Zan\Framework\Components\Nsq\Contract\ConnDelegate;
use Zan\Framework\Components\Nsq\Utils\Backoff;
use Zan\Framework\Foundation\Contract\Async;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;
use Zan\Framework\Utilities\Types\Time;

class Connection implements Async
{
    /**
     * @var SwooleClient
     */
    private $client;

    /**
     * @var callable usage for async
     */
    private $callback;

    /**
     * @var ConnDelegate
     */
    private $delegate;

    private $host;

    private $port;

    private $lookupAddr;

    private $isBusy = false;

    private $isDisposable = false;

    private $isConnected = false;

    private $isWaitingClose = false;

    /**
     * 当前处理中消息数量
     * @var int
     * recv FrameTypeMessage ++
     * msgFinish --
     * msgRequeue --
     */
    private $messagesInFlight = 0;

    /**
     * nsqd尚未推送过来的消息数量
     * @var int
     * recv FrameTypeMessage --
     * consumer sendRdy
     */
    private $rdyCount = 0;

    private $lastRdyCount = 0;

    private $lastMsgTimestamp;

    /**
     * Disposable Connection
     * @param $host
     * @param $port
     * @param int $lifeCycle
     * @return \Generator
     * @throws \Exception
     *  Connection 是 Async 对象, 这里用数组返回
     *  list($conn) = (yield Connection::getDisposable("10.9.80.209", 4150, $liftCycle));
     */
    public static function getDisposable($host, $port, $lifeCycle = 3000)
    {
        $conn = new static($host, $port);
        yield $conn->connect();
        $conn->isDisposable = true;
        Timer::after($lifeCycle, [$conn, "tryClose"]);
        yield [$conn];
    }

    /**
     * Connection constructor.
     * @param $host
     * @param $port
     * @param ConnDelegate $delegate
     */
    public function __construct($host, $port, ConnDelegate $delegate = null)
    {
        $this->host = $host;
        $this->port = $port;
        if ($delegate === null) {
            $this->delegate = NopConnDelegate::getInstance();
        } else {
            $this->delegate = $delegate;
        }
        $this->createClient();
    }

    private function createClient()
    {
        $this->client = new SwooleClient(SWOOLE_TCP, SWOOLE_SOCK_ASYNC);
        $this->client->set([
            "open_length_check" => true,
            "package_length_type" => 'N',
            "package_length_offset" => 0,
            "package_body_offset" => 4,
            "package_max_length" => NsqConfig::getPacketSizeLimit(),
            "socket_buffer_size" => NsqConfig::getSocketBufferSize(),
            "open_tcp_nodelay" => true,
        ]);
        $this->client->on("connect", $this->onConnect());
        $this->client->on("receive", [$this, "onIdentity"]); // Cannot destroy active lambda function
        $this->client->on("error", $this->onError());
        $this->client->on("close", $this->onClose());
    }

    public function setDelegate(ConnDelegate $delegate)
    {
        $this->delegate = $delegate;
    }

    public function isDisposable()
    {
        return $this->isDisposable;
    }

    public function tryTake()
    {
        if ($this->isClosing() || $this->isBusy === true) {
            return false;
        } else {
            $this->isBusy = true;
            return true;
        }
    }

    public function tryRelease()
    {
        if ($this->isClosing() || $this->isBusy === false) {
            return false;
        } else {
            $this->isBusy = false;
            return true;
        }
    }

    public function connect()
    {
        if ($this->isConnected) {
            return;
        }
        $timeout = NsqConfig::getNsqdConnectTimeout();
        Timer::after($timeout, $this->onConnectTimeout(), $this->getConnectTimeoutTimerId());
        $this->client->connect($this->host, $this->port);
        yield $this;
    }

    private function onConnectTimeout()
    {
        return function () {
            $timeout = NsqConfig::getNsqdConnectTimeout();
            call_user_func($this->callback, null, new NsqException("nsq({$this->host}:{$this->port}) connect timeout [time=$timeout]"));
        };
    }

    public function writeCmd($cmd)
    {
        $this->write($cmd);
    }

    private function writeMessage(Message $msg, $cmd, $success = true, $backoff = false)
    {
        $this->messagesInFlight--;

        if ($success) {
            $this->delegate->onMessageFinished($this, $msg);
            $this->delegate->onResume($this); // resume backoff
        } else {
            $this->delegate->onMessageRequeued($this, $msg);
            if ($backoff) {
                $this->delegate->onBackoff($this); // enter backoff
            } else {
                $this->delegate->onContinue($this);
            }
        }

        $this->writeCmd($cmd);
    }

    public function getHost()
    {
        return $this->host;
    }

    public function getPort()
    {
        return $this->port;
    }

    public function getAddr()
    {
        return "$this->host:$this->port";
    }

    /**
     * 获取当前RDY余量
     * @return int
     */
    public function getRemainRDY()
    {
        return $this->rdyCount;
    }

    /**
     * returns the previously set RDY count
     * @return int
     */
    public function lastRDY()
    {
        return $this->lastRdyCount;
    }

    /**
     * stores the specified RDY count
     * @param $rdy
     */
    public function setRDY($rdy)
    {
        $this->rdyCount = $rdy;
        $this->lastRdyCount = $rdy;
    }

    /**
     * returns the nsqd negotiated maximum
     * @return int
     */
    public function maxRDY()
    {
        return NsqConfig::getMaxRDYCount();
    }

    public function getMsgInFlight()
    {
        return $this->messagesInFlight;
    }

    /**
     * returns a time.Time representing the time at which the last message was received
     * @return mixed
     */
    public function lastMessageTime()
    {
        return $this->lastMsgTimestamp;
    }

    /**
     * @return string|null
     */
    public function getLookupAddr()
    {
        return $this->lookupAddr;
    }

    /**
     * @param string $lookupAddr
     */
    public function setLookupAddr($lookupAddr)
    {
        $this->lookupAddr = $lookupAddr;
    }

    public function onMessageFinish(Message $msg)
    {
        $this->writeMessage($msg, Command::finish($msg));
    }

    public function onMessageRequeue(Message $msg, $delay = -1, $backoff = false)
    {
        if ($delay === -1) {
            $c = NsqConfig::getMessageBackoff();
            $delay = Backoff::calculate($msg->getAttempts(), $c["min"], $c["max"], $c["factor"], $c["jitter"]);
        }
        $this->writeMessage($msg, Command::requeue($msg, $delay), false, $backoff);
    }

    public function onMessageTouch(Message $msg)
    {
        $this->writeCmd(Command::touch($msg));
    }

    private function onConnect()
    {
        return function (/*SwooleClient $client*/) {
            $this->isConnected = true;
            try {
                Timer::clearAfterJob($this->getConnectTimeoutTimerId());
                $this->write(Frame::MAGIC_V2);
                $this->writeCmd(Command::identify());
            } catch (\Exception $ex) {
                $this->onIOError($ex->getMessage());
            }
        };
    }

    public function onIdentity(/** @noinspection PhpUnusedParameterInspection */
        SwooleClient $client, $bytes)
    {
        try {
            $this->delegate->onReceive($this, $bytes);
            $frame = new Frame($bytes);
            $this->confirmIdentity($frame);
        } catch (\Exception $ex) {
            sys_echo("nsq({$this->host}:{$this->port}) identity fail, {$ex->getMessage()}");
            $this->onIOError($ex->getMessage());
        }
    }

    private function confirmIdentity(Frame $frame)
    {
        $frameType = $frame->getType();
        $frameBody = $frame->getBody();
        if ($frameType !== Frame::FrameTypeResponse) {
            goto fail;
        }

        $enableNegotiation = NsqConfig::getIdentity()["feature_negotiation"];
        $isJson = $frameBody[0] === '{' && $enableNegotiation;
        if ($isJson) {
            $idResp = json_decode($frameBody, true, JSON_BIGINT_AS_STRING);
            NsqConfig::negotiateIdentity($idResp);
            goto success;

        } else {
            if ($frameBody === "OK") {
                goto success;
            } else {
                goto fail;
            }
        }

        fail:
        call_user_func($this->callback, null, new NsqException("[frameType=$frameType, frameBody=$frameBody]"));
        return;

        success:
        $this->client->on("receive", [$this, "onReceive"]);
        call_user_func($this->callback, $this, null);
        return;
    }

    public function onReceive(/** @noinspection PhpUnusedParameterInspection */
        SwooleClient $client, $bytes)
    {
        try {
            $this->delegate->onReceive($this, $bytes);
            $frame = new Frame($bytes);

            $isHeartbeat = $frame->getType() === Frame::FrameTypeResponse
                && $frame->getBody() === Frame::HEARTBEAT;

            if ($isHeartbeat) {
                $this->delegate->onHeartbeat($this);
                $this->writeCmd(Command::nop());
                return;
            }

            Task::execute($this->dispatchFrame($frame));
        } catch (\Exception $ex) {
            sys_echo("nsq({$this->host}:{$this->port}) recv or handle fail, {$ex->getMessage()}");
            echo_exception($ex);
            $this->onIOError($ex->getMessage());
        }
    }

    private function dispatchFrame(Frame $frame)
    {
        try {
            yield $this->doDispatchFrame($frame);
        } catch (\Exception $ex) {
            sys_echo("nsq({$this->host}:{$this->port}) dispatchFrame exception: {$ex->getMessage()}");
            echo_exception($ex);
        }
    }

    private function doDispatchFrame(Frame $frame)
    {
        switch ($frame->getType()) {
            case Frame::FrameTypeResponse:
                yield $this->delegate->onResponse($this, $frame->getBody());
                break;

            case Frame::FrameTypeMessage:
                try {
                    $msg = new Message($frame->getBody(), new ConnMsgDelegate($this));
                    yield $this->delegate->onMessage($this, $msg);
                } finally {
                    $this->rdyCount--;
                    $this->messagesInFlight++;
                    $this->lastMsgTimestamp = Time::stamp();
                }
                break;

            case Frame::FrameTypeError:
                yield $this->delegate->onError($this, $frame->getBody());
                break;

            default:
                $msg = "nsq({$this->host}:{$this->port}) receive unknown frame type {$frame->getType()}";
                sys_echo($msg);
        }
    }

    private function onError()
    {
        /**
         * swoole Client.c
         * static int swClient_onError(swReactor *reactor, swEvent *event)
         * {
         *     if (cli->onError)
         *     {
         *          cli->onError(cli);
         *     }
         *     int ret = cli->close(cli);
         * }
         * onError 之后, swoole会主动触发onClose, 所以, reconnect 只在onClose中来做就可以了
         */
        return function(/*SwooleClient $client*/) {
            $this->onIOError("swoole client onError");
        };
    }

    private function onClose()
    {
        return function(/*SwooleClient $client*/) {
            Timer::clearAfterJob($this->getConnectTimeoutTimerId());
            $this->isConnected = false;
            try {
                $this->delegate->onClose($this);
            } catch (\Exception $ex) {
                sys_echo("nsq({$this->host}:{$this->port}) onClose exception: {$ex->getMessage()}");
            }
        };
    }

    private function write($payload, $ignoreError = false)
    {
        if ($this->isClosing()) {
            sys_echo("nsq({$this->host}:{$this->port}) write \"$payload\" fail, because connection is closing");
            return;
        }

        $this->delegate->onSend($this, $payload);
        $ok = $this->client->send($payload);
        // 防止异常情况send always fail, 发送CLS命令导致递归
        if (!$ok && !$ignoreError) {
            $this->onIOError("swoole client send fail");
        }
    }

    private function onIOError($reason)
    {
        $errCode = $this->client->errCode;
        if ($errCode) {
            $errMsg = swoole_strerror($errCode);
            $this->delegate->onIOError($this, new NsqException("nsqd({$this->host}:{$this->port}) IOError: $reason [errCode=$errCode, errMsg=$errMsg]"));
            $this->client->errCode = 0;
        } else {
            $this->delegate->onIOError($this, new NsqException("nsqd({$this->host}:{$this->port}) IOError: $reason"));
        }
    }

    public function isClosing()
    {
        return $this->isWaitingClose || !isset($this->client) || !$this->client->isConnected();
    }

    public function tryClose()
    {
        if ($this->isClosing()) {
            return false;
        }

        try {
            $this->prepareClose();
        } catch (\Exception $ex) {
            sys_echo("nsq({$this->host}:{$this->port}) tryClose exception:  {$ex->getMessage()}");
        }

        if ($this->messagesInFlight === 0) {
            $this->immediatelyClose();
            return true;
        } else {
            $this->delayingClose(NsqConfig::getDelayingCloseTime());
            return false;
        }
    }

    private function prepareClose()
    {
        $this->write(Command::startClose(), true);
        $this->client->sleep();
        $this->isWaitingClose = true;
    }

    private function immediatelyClose()
    {
        $this->client->close();
        unset($this->client);
        $this->isWaitingClose = false;
    }

    private function delayingClose($delayTime)
    {
        sys_echo("nsq connection ({$this->host}:{$this->port}) delaying close after {$delayTime}ms, $this->messagesInFlight outstanding messages");
        Timer::after($delayTime, function() {
            $this->immediatelyClose();
        });
    }

    public function execute(callable $callback, $task)
    {
        $this->callback = $callback;
    }

    public function ping()
    {
        if ($this->isClosing()) {
            return false;
        }

        try {
            // NOTE: there is no response
            $this->writeCmd(Command::nop());
            return true;
        } catch (\Exception $ex) {
            return false;
        }
    }

    private function getConnectTimeoutTimerId()
    {
        return sprintf("%s_%s_connect_timeout", spl_object_hash($this), __CLASS__);
    }
}