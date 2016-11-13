<?php

namespace Zan\Framework\Components\Nsq;

use Zan\Framework\Components\Contract\Nsq\MsgDelegate;

class ConnMsgDelegate implements MsgDelegate
{
    /**
     * @var Connection
     */
    private $conn;

    public function __construct(Connection $conn)
    {
        $this->conn = $conn;
    }

    /**
     * OnFinish is called when the Finish() method
     * is triggered on the Message
     * @param Message $message
     * @return void
     */
    public function onFinish(Message $message)
    {
        $this->conn->onMessageFinish($message);
    }

    /**
     * OnRequeue is called when the Requeue() method
     * is triggered on the Message
     * @param Message $message
     * @param int $delay
     * @param bool $backoff
     * @return void
     */
    public function onRequeue(Message $message, $delay, $backoff)
    {
        $this->conn->onMessageRequeue($message, $delay, $backoff);
    }

    /**
     * OnTouch is called when the Touch() method
     * is triggered on the Message
     * @param Message $message
     * @return void
     */
    public function onTouch(Message $message)
    {
        $this->conn->onMessageTouch($message);
    }
}