<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Foundation\Contract\Async;
use Zan\Framework\Foundation\Core\Debug;

class Producer implements ConnDelegate, Async
{
    const StateInit = 0;
    const StateDisconnected = 1;
    const StateConnected = 2;

    /**
     * @var Connection
     */
    private $conn;

    private $state;

    private $host;

    private $port;

    private $callback;

    public function __construct($host, $port)
    {
        $this->host = $host;
        $this->port = $port;
        $this->state = static::StateInit;
    }

    /**
     * Ping causes the Producer to connect to it's configured nsqd (if not already
     * connected) and send a `Nop` command
     *
     * This method can be used to verify that a newly-created Producer instance is
     * configured correctly, rather than relying on the lazy "connect on Publish"
     * behavior of a Producer.
     */
    public function ping()
    {
        if ($this->state !== static::StateConnected) {
            yield $this->connect();
        }

        yield $this->conn->ping();
    }

    public function connect()
    {
        switch ($this->state) {
            case static::StateInit:
                break;
            case static::StateConnected:
                return;
            default:
                throw new NsqException("not connected");
        }

        $this->conn = new Connection($this->host, $this->port, $this);
        yield $this->conn->connect();
        $this->state = static::StateConnected;
    }

    public function stop()
    {
        return $this->conn->tryClose();
    }

    /**
     * Publish a message to a topic
     * @param string $topic
     * @param string $body
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
    public function publish($topic, $body)
    {
        Command::checkTopicChannelName($topic);
        if ($this->state !== static::StateConnected) {
            yield $this->connect();
        }
        $this->conn->writeCmd(Command::publish($topic, $body));
        yield $this;
    }

    /**
     * Publish multiple messages to a topic (atomically)
     * (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
     * @param $topic
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
    public function multiPublish($topic, array $messages)
    {
        Command::checkTopicChannelName($topic);
        if ($this->state !== static::StateConnected) {
            yield $this->connect();
        }
        $this->conn->writeCmd(Command::multiPublish($topic, $messages));
        yield $this;
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
        call_user_func($this->callback, $bytes, null);
        // TODO 释放连接
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
        call_user_func($this->callback, null, new NsqException($bytes));
        // TODO 释放连接
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
        $this->conn->tryClose();
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
        call_user_func($this->callback, null, new NsqException("conn closed"));
    }

    public function onReceive(Connection $conn, $bytes) {
        if (Debug::get()) {
            // sys_echo("nsq({$conn->getAddr()}) rev:" . str_replace("\n", "\\n", $bytes));
        }
    }
    public function onSend(Connection $conn, $bytes) {
        if (Debug::get()) {
            sys_echo("nsq({$conn->getAddr()}) send:" . str_replace("\n", "\\n", $bytes));
        }
    }

    public function execute(callable $callback, $task)
    {
        $this->callback = $callback;
    }

    public function onHeartbeat(Connection $conn) {}
    public function onMessage(Connection $conn, Message $msg) {}
    public function onMessageFinished(Connection $conn, Message $msg) {}
    public function onMessageRequeued(Connection $conn, Message $msg) {}
    public function onBackoff(Connection $conn) {}
    public function onContinue(Connection $conn) {}
    public function onResume(Connection $conn) {}
}