<?php

namespace Zan\Framework\Components\Nsq;

use swoole_client as SwooleClient;
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

    /**
     * @var bool
     */
    private $waitingClose;

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
     * Connection constructor.
     * @param $host
     * @param $port
     * @param ConnDelegate $connDelegate
     */
    public function __construct($host, $port, ConnDelegate $connDelegate)
    {
        $this->delegate = $connDelegate;
        $this->host = $host;
        $this->port = $port;
        $this->createClient();
    }

    private function createClient()
    {
        $this->client = new SwooleClient(SWOOLE_TCP, SWOOLE_SOCK_ASYNC);
        $this->client->set([
            "open_length_check"     => true,
            "package_length_type"   => 'N',
            "package_length_offset" => 0,
            "package_body_offset"   => 4,
            "package_max_length"    => NsqConfig::getPacketSizeLimit(),
            "socket_buffer_size"    => NsqConfig::getSocketBufferSize(),
            "open_tcp_nodelay"      => true,
        ]);
        $this->client->on("connect",    $this->onConnect());
        $this->client->on("receive",    [$this, "onIdentity"]); // Cannot destroy active lambda function
        $this->client->on("error",      $this->onError());
        $this->client->on("close",      $this->onClose());
    }

    public function connect()
    {
        $timeout = NsqConfig::getNsqdConnectTimeout();
        Timer::after($timeout, $this->onConnectTimeout(), $this->getTimeoutTimerId());
        $this->client->connect($this->host, $this->port);
        yield $this;
    }

    private function onConnectTimeout()
    {
        return function() {
            $timeout = NsqConfig::getNsqdConnectTimeout();
            call_user_func($this->callback, null, new NsqException("nsq({$this->host}{$this->port})connect timeout [time=$timeout]"));
        };
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

    public function writeCmd($cmd)
    {
        $this->write($cmd);
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
        return function(/*SwooleClient $client*/) {
            try {
                Timer::clearAfterJob($this->getTimeoutTimerId());
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
            // HEARTBEAT
            if ($frame->getType() === Frame::FrameTypeResponse) {
                if ($frame->getBody() === Frame::HEARTBEAT) {
                    $this->delegate->onHeartbeat($this);
                    $this->writeCmd(Command::nop());
                    return;
                }
            }

            $frameTask = new Task($this->dispatchFrame($frame));
            $frameTask->run();
        } catch (\Exception $ex) {
            sys_echo("nsq({$this->host}:{$this->port}) recv or handle fail, {$ex->getMessage()}");
            echo_exception($ex);
            $this->onIOError($ex->getMessage());
        }
    }

    private function dispatchFrame(Frame $frame)
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
                throw new NsqException("receive unknown frame type {$frame->getType()}");
        }
    }

    private function onError()
    {
        return function(/*SwooleClient $client*/) {
            $this->onIOError("swoole client error");
        };
    }

    private function onClose()
    {
        return function(/*SwooleClient $client*/) {
            $this->delegate->onClose($this);
        };
    }

    private function write($payload)
    {
        $this->delegate->onSend($this, $payload);
        if (!$this->client->send($payload)) {
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
        return $this->waitingClose || !isset($this->client) || !$this->client->isConnected();
    }

    public function tryClose()
    {
        if ($this->isClosing()) {
            return false;
        }

        $this->prepareClose();

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
        $this->writeCmd(Command::startClose());
        $this->client->sleep();
        $this->waitingClose = true;
    }

    private function immediatelyClose()
    {
        $this->client->close();
        unset($this->client);
        $this->waitingClose = false;
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

    private function getTimeoutTimerId()
    {
        return sprintf("%s_%s_connect_timeout", spl_object_hash($this), __CLASS__);
    }
}