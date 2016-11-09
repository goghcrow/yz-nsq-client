<?php

use Zan\Framework\Components\Nsq\Command;
use Zan\Framework\Components\Nsq\ConnDelegate;
use Zan\Framework\Components\Nsq\Connection;
use Zan\Framework\Components\Nsq\Consumer;
use Zan\Framework\Components\Nsq\Lookup;
use Zan\Framework\Components\Nsq\Message;
use Zan\Framework\Components\Nsq\MsgHandler;
use Zan\Framework\Components\Nsq\NsqConfig;
use Zan\Framework\Components\Nsq\NsqException;
use Zan\Framework\Components\Nsq\Producer;
use Zan\Framework\Components\Nsq\PoolProducer;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/../../vendor/autoload.php";
NsqConfig::init();
\Zan\Framework\Foundation\Core\Debug::detect();

// curl -X POST "http://sqs-qa.s.qima-inc.com:4151/channel/create?topic=zan_mqworker_test&channel=ch1"
function task() {
    $lookup = new Lookup("http://sqs-qa.s.qima-inc.com:4161");
    $producer = new PoolProducer($lookup);
    $recv = (yield $producer->publish("zan_mqworker_test", "你好"));
    echo $recv, "\n";
    $recv = (yield $producer->multiPublish("zan_mqworker_test", range(1, 10)));
}

Task::execute(task());