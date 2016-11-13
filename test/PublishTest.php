<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Producer;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";


function taskPub()
{
    $topic = "zan_mqworker_test";

    $oneMsg = "hello";
    $multiMsgs = [
        "hello",
        "hi",
    ];

    /* @var Producer $producer */
    $ok = (yield SQS::publish($topic, $oneMsg));
    $ok = (yield SQS::publish($topic, "hello", "hi"));
    $ok = (yield SQS::publish($topic, ...$multiMsgs));
}

Task::execute(taskPub());