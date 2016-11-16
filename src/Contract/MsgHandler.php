<?php

namespace Zan\Framework\Components\Nsq\Contract;

use Zan\Framework\Components\Nsq\Consumer;
use Zan\Framework\Components\Nsq\Message;


/**
 * Interface MessageHandler
 * @package Zan\Framework\Components\Nsq
 *
 * MessageHandler is the message processing interface for Consumer.
 * Implement this interface for handlers that return whether or not message
 * processing completed successfully.
 *
 */
interface MsgHandler
{
    /**
     * @param Message $message
     * @param Consumer $consumer
     * @return bool When the return value is == true Consumer will automatically handle FINishing.
     *
     * When the return value !== false Consumer will automatically handle FINishing.
     * When the returned value is ==- false Consumer will automatically handle REQueing.
     * When exception caught Consumer will automatically handle REQueing.
     */
    public function handleMessage(Message $message, Consumer $consumer);

    /**
     * will be called when a message is deemed "failed"
     * (i.e. the number of attempts exceeded the Consumer specified MaxAttemptCount)
     * @param Message $message
     * @param Consumer $consumer
     * @return mixed
     */
    public function logFailedMessage(Message $message, Consumer $consumer);
}