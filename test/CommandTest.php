<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Command;
use Zan\Framework\Components\Nsq\Contract\MsgDelegate;
use Zan\Framework\Components\Nsq\Message;

require_once __DIR__ . "/boot.php";

if (!class_exists("NopMsgDelegate")) {
    class NopMsgDelegate implements MsgDelegate
    {
        public function onFinish(Message $message) {}
        public function onRequeue(Message $message, $delay, $backoff) {}
        public function onTouch(Message $message) {}
    }
}

$msg = new Message(Message::pack(1, 1, "body")->readFull(), new NopMsgDelegate());

Command::identify();
Command::nop();
Command::subscribe("topic", "channel");
Command::publish("topic", "body");
Command::multiPublish("topic", ["body1", "body2"]);
Command::ready(1);
Command::touch($msg);
Command::requeue($msg);
Command::finish($msg);
Command::startClose();