<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Contract\MsgDelegate;
use Zan\Framework\Components\Nsq\Message;

require_once __DIR__ . "/boot.php";

class NopMsgDelegate implements MsgDelegate
{
    public function onFinish(Message $message) {}
    public function onRequeue(Message $message, $delay, $backoff) {}
    public function onTouch(Message $message) {}
}

$id = substr(md5(__FILE__), 0, 16);
$attempts = 4;
$payload = "hello,  o(╯□╰)o";
$bytes = Message::pack($id, 4, $payload);
$msg = new Message($bytes, new NopMsgDelegate());
assert($msg->getId() === $id);
assert($msg->getAttempts() === $attempts);
assert($msg->getBody() === $payload);

assert($msg->hasResponsed() === false);
$msg->touch();
assert($msg->hasResponsed() === false);
$msg->finish();
assert($msg->hasResponsed() === true);
$msg->requeue(1000);