<?php

namespace Zan\Framework\Components\Nsq\Contract;


use Zan\Framework\Components\Nsq\Message;

interface MsgDelegate
{
    /**
     * OnFinish is called when the Finish() method
     * is triggered on the Message
     * @param Message $message
     * @return mixed
     */
    public function onFinish(Message $message);

    /**
     * OnRequeue is called when the Requeue() method
     * is triggered on the Message
     * @param Message $message
     * @param int $delay
     * @param bool $backoff
     * @return mixed
     */
    public function onRequeue(Message $message, $delay, $backoff);

    /**
     * OnTouch is called when the Touch() method
     * is triggered on the Message
     * @param Message $message
     * @return mixed
     */
    public function onTouch(Message $message);
}