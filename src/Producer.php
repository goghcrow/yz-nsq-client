<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Components\Nsq\Contract\ConnDelegate;
use Zan\Framework\Components\Nsq\Contract\NsqdDelegate;
use Zan\Framework\Foundation\Contract\Async;
use Zan\Framework\Foundation\Core\Debug;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;
use ZanPHP\Contracts\Trace\Constant;
use ZanPHP\Trace\Trace;


class Producer implements ConnDelegate, NsqdDelegate, Async
{
    // 每个topic共享一个Producer实例
    // 单独保存parallel条件下每个async callback
    private $callbacks = [];

    private $topic;

    private $lookup;

    private $stats;

    /** @var  Trace */
    private $trace;
    private $traceHandle;
    private $ctx;

    public function __construct($topic, $maxConnectionNum = 1)
    {
        $this->topic = Command::checkTopicChannelName($topic);
        $this->lookup = new Lookup($this->topic, Lookup::W, $maxConnectionNum);
        $this->lookup->setNsqdDelegate($this);

        $this->stats = [
            "messagesPublished" => 0,
        ];
    }

    public function connectToNSQLookupds(array $addresses)
    {
        foreach ($addresses as $address) {
            yield $this->connectToNSQLookupd($address);
        }
    }

    public function connectToNSQLookupd($address)
    {
        yield $this->lookup->connectToNSQLookupd($address);
    }

    public function queryNSQLookupd()
    {
        yield $this->lookup->queryLookupd();
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

    public function getNsqdConns()
    {
        return $this->lookup->getNSQDConnections();
    }

    /**
     * @return \Generator
     */
    private function take()
    {
        yield $this->lookup->take();
    }

    private function release(Connection $conn)
    {
        return $this->lookup->release($conn);
    }

    /**
     * @param Connection $conn
     * @return \Generator
     */
    private function reconnectToNSQD(Connection $conn)
    {
        yield $this->lookup->reconnect($conn);
    }

    /**
     * 统计信息
     * @return array
     */
    public function stats()
    {
        return $this->stats + $this->lookup->stats();
    }

    /**
     * Publish a message to a topic
     * @param string $message
     * @param MessageParam $params
     * @throws NsqException
     * @return \Generator
     *
     * Success Response:
     *  OK
     * Error Responses (NsqException):
     *  E_INVALID
     *  E_BAD_TOPIC
     *  E_BAD_MESSAGE
     *  E_MPUB_FAILED
     */
    public function publish($message, $params)
    {
        $this->trace = (yield getContext('trace'));

        if ($this->trace) {
            $this->traceHandle = $this->trace->transactionBegin(Constant::NSQ_PUB, $this->topic);
            $this->ctx = $message;
        }
        /* @var Connection $conn */
        list($conn) = (yield $this->take());
        $partitionId = $conn->getPartition();
        $paramArray = null;
        if (is_object($params)) {
            $paramArray = $params->toArray();
        }
        if (empty($paramArray)) {
            $cmd = Command::publish($this->topic, $message, $partitionId);
        } else {
            $cmd = Command::publishWithExtends($this->topic, $message, $partitionId, $paramArray);
        }
        $conn->writeCmd($cmd);
        $this->stats["messagesPublished"]++;

        $timeout = NsqConfig::getPublishTimeout();
        $onTimeout = $this->onPublishTimeout($conn);
        $timerId = $this->getPublishTimeoutTimerId($conn);

        Timer::after($timeout, $onTimeout, $timerId);

        yield setContext("conn", $conn);
        yield $this;
    }

    /**
     * Publish multiple messages to a topic (atomically)
     * (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
     * @param array $messages
     * @throws NsqException
     * @return \Generator
     *
     * Success Response:
     *  OK
     * Error Responses (NsqException):
     *  E_INVALID
     *  E_BAD_TOPIC
     *  E_BAD_BODY
     *  E_BAD_MESSAGE
     *  E_MPUB_FAILED
     */
    public function multiPublish(array $messages)
    {
        $this->trace = (yield getContext('trace'));

        if ($this->trace) {
            $this->traceHandle = $this->trace->transactionBegin(Constant::NSQ_PUB, $this->topic);
            $this->ctx = var_export($messages, true);
        }
        /* @var Connection $conn */
        list($conn) = (yield $this->take());
        $conn->writeCmd(Command::multiPublish($this->topic, $messages));

        $this->stats["messagesPublished"] += count($messages);

        $timeout = NsqConfig::getPublishTimeout();
        $onTimeout = $this->onPublishTimeout($conn);
        $timerId = $this->getPublishTimeoutTimerId($conn);

        Timer::after($timeout, $onTimeout, $timerId);

        yield setContext("conn", $conn);
        yield $this;
    }

    /**
     * onConnected is called when nsqd connects
     * @param Connection $conn
     * @return void
     */
    public function onConnect(Connection $conn)
    {
        $conn->setDelegate($this);
    }

    /**
     * OnResponse is called when the connection
     * receives a FrameTypeResponse from nsqd
     * @param Connection $conn
     * @param string $bytes
     * @return void
     */
    public function onResponse(Connection $conn, $bytes)
    {
        $this->onPublishResponse($conn, $bytes);

        if ($bytes === "CLOSE_WAIT") {
            // nsqd ack客户端的StartClose, 准备关闭
            // 可以假定不会再收到任何nsqd的消息
            $conn->tryClose();
        }
    }

    /**
     * OnError is called when the connection
     * receives a FrameTypeError from nsqd
     * @param Connection $conn
     * @param $bytes
     * @return void
     */
    public function onError(Connection $conn, $bytes)
    {
        $this->onPublishResponse($conn, "CONN_ERROR", new NsqException($bytes));
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
        $this->onPublishResponse($conn, "IO_ERROR", $ex);

        try {
            $this->lookup->removeConnection($conn);
            $conn->tryClose();
        } catch (\Throwable $ignore) {
            sys_echo("nsq({$conn->getAddr()}) onIOError exception: {$ex->getMessage()}");
        } catch (\Exception $ignore) {
            sys_echo("nsq({$conn->getAddr()}) onIOError exception: {$ex->getMessage()}");
        }
    }

    /**
     * OnClose is called when the connection
     * closes, after all cleanup
     * @param Connection $conn
     * @throws NsqException
     * @return void
     */
    public function onClose(Connection $conn)
    {
        $this->onPublishResponse($conn, "CONN_CLOSED", new NsqException("connection close, maybe disposable connection timeout"));

        if (!$conn->isDisposable()) {
            $this->lookup->removeConnection($conn);

            $remainConnNum = count($this->getNsqdConns());
            sys_echo("nsq({$conn->getAddr()}) producer onClose: there are $remainConnNum connections left alive");

            if ($this->lookup->isStopped()/* && $remainConnNum === 0*/) {
                return;
            }

            Task::execute($this->reconnectToNSQD($conn));
        }
    }

    private function onPublishResponse(Connection $conn, $retval, \Exception $ex = null)
    {
        Timer::clearAfterJob($this->getPublishTimeoutTimerId($conn));

        if ($this->trace) {
            if ($ex === null) {
                $this->trace->commit($this->traceHandle, Constant::SUCCESS);
            } else {
                $this->trace->commit($this->traceHandle, $ex->getMessage()." ".$this->ctx);
            }
        }
        $this->release($conn);

        $hash = spl_object_hash($conn);
        if (isset($this->callbacks[$hash])) {
            $callback = $this->callbacks[$hash];
            // NOTICE!!!
            // 这里必须首先unset掉, 因为 call_user_func唤醒task, 内部可能仍然有publish异步操作
            // 则可能会执行到execute中, 新的callback将会覆盖旧的callback
            unset($this->callbacks[$hash]);
            call_user_func($callback, $retval, $ex);
        }
    }

    private function onPublishTimeout(Connection $conn)
    {
        return function() use($conn) {
            if ($this->trace) {
                $this->trace->commit($this->traceHandle, "publish timeout: ".$this->ctx);
                $this->trace = null;
            }
            $connHash = spl_object_hash($conn);
            if (isset($this->callbacks[$connHash])) {
                $callback = $this->callbacks[$connHash];
                call_user_func($callback, "TIME_OUT", new NsqException("publish timeout"));
                unset($this->callbacks[$connHash]);

                $conn->tryClose(true);
            }
        };
    }

    /**
     * @param callable $callback
     * @param Task $task
     */
    public function execute(callable $callback, $task)
    {
        $conn = $task->getContext()->get("conn");
        $connHash = spl_object_hash($conn);
        if (isset($this->callbacks[$connHash])) {
            assert(false); // for debug
        } else {
            $this->callbacks[$connHash] = $callback;
        }
    }

    private function getPublishTimeoutTimerId(Connection $conn)
    {
        return sprintf("%s_%s_public_time_out", spl_object_hash($this), spl_object_hash($conn));
    }

    public function onReceive(Connection $conn, $bytes) {
//        if (Debug::get()) {
//            sys_echo("nsq({$conn->getAddr()}) recv:" . str_replace("\n", "\\n", $bytes));
//        }
    }

    public function onSend(Connection $conn, $bytes) {
//        if (Debug::get()) {
//            sys_echo("nsq({$conn->getAddr()}) send:" . str_replace("\n", "\\n", $bytes));
//        }
    }

    public function onHeartbeat(Connection $conn) {}
    public function onMessage(Connection $conn, Message $msg) {}
    public function onMessageFinished(Connection $conn, Message $msg) {}
    public function onMessageRequeued(Connection $conn, Message $msg) {}
    public function onBackoff(Connection $conn) {}
    public function onContinue(Connection $conn) {}
    public function onResume(Connection $conn) {}
}
