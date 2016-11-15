<?php

namespace Zan\Framework\Components\Nsq;

use Zan\Framework\Components\Contract\Nsq\ConnDelegate;
use Zan\Framework\Foundation\Core\Debug;
use Zan\Framework\Utilities\DesignPattern\Singleton;

class NopConnDelegate implements ConnDelegate
{
    use Singleton;

    public function onResponse(Connection $conn, $bytes)
    {
        sys_echo("nsq({$conn->getAddr()}) onResponse :$bytes");
    }

    public function onError(Connection $conn, $bytes)
    {
        sys_echo("nsq({$conn->getAddr()}) onError :$bytes");
    }

    public function onMessage(Connection $conn, Message $msg)
    {
        sys_echo("nsq({$conn->getAddr()}) onMessage :{$msg->__toString()}");

    }
    public function onMessageFinished(Connection $conn, Message $msg)
    {
        sys_echo("nsq({$conn->getAddr()}) onMessageFinished");
    }

    public function onMessageRequeued(Connection $conn, Message $msg)
    {
        sys_echo("nsq({$conn->getAddr()}) onMessageRequeued");
    }

    public function onBackoff(Connection $conn)
    {
        sys_echo("nsq({$conn->getAddr()}) onBackoff");
    }

    public function onContinue(Connection $conn)
    {
        sys_echo("nsq({$conn->getAddr()}) onContinue");
    }

    public function onResume(Connection $conn)
    {
        sys_echo("nsq({$conn->getAddr()}) onResume");
    }

    public function onIOError(Connection $conn, \Exception $ex)
    {
        sys_echo("nsq({$conn->getAddr()}) onIOError"); echo_exception($ex);
    }

    public function onHeartbeat(Connection $conn)
    {
        sys_echo("nsq({$conn->getAddr()}) onHeartbeat");
    }

    public function onClose(Connection $conn)
    {
        sys_echo("nsq({$conn->getAddr()}) onClose");
    }

    public function onReceive(Connection $conn, $bytes)
    {
        if (Debug::get()) {
            sys_echo("nsq({$conn->getAddr()}) recv:" . str_replace("\n", "\\n", $bytes));
        }
    }

    public function onSend(Connection $conn, $bytes)
    {
        if (Debug::get()) {
            sys_echo("nsq({$conn->getAddr()}) send:" . str_replace("\n", "\\n", $bytes));
        }
    }
}