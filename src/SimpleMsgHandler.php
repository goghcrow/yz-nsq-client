<?php

namespace Zan\Framework\Components\Nsq;



use Zan\Framework\Components\Nsq\Contract\MsgHandler;

class SimpleMsgHandler implements MsgHandler
{
    private $callable;

    public function __construct($callable)
    {
        $this->callable = $callable;
    }

    /**
     * @param Message $message
     * @param Consumer $consumer
     * @return bool When the return value is == true Consumer will automatically handle FINishing.
     *
     * When the return value is == true Consumer will automatically handle FINishing.
     * When the returned value is == false Consumer will automatically handle REQueing.
     */
    public function handleMessage(Message $message, Consumer $consumer)
    {
        yield call_user_func($this->callable, $message, $consumer);
    }

    /**
     * will be called when a message is deemed "failed"
     * (i.e. the number of attempts exceeded the Consumer specified MaxAttemptCount)
     * @param Message $message
     * @param Consumer $consumer
     * @return mixed
     */
    public function logFailedMessage(Message $message, Consumer $consumer)
    {
        sys_echo("!!! Message Fail !!! \n $message");
    }
}