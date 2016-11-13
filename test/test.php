<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Consumer;
use Zan\Framework\Components\Nsq\Message;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";

$taskSub = function() {
    try {
        $topic = "zan_mqworker_test";
        $ch = "ch1";
        /* @var Consumer $consumer */
        $consumer = (yield SQS::subscribe($topic, $ch, function(Message $msg, Consumer $consumer) {
            // echo $msg, "\n";
            yield taskSleep(1000);
            yield true;
        }, 1));
    } catch (\Exception $ex) {
        echo_exception($ex);
    }
};
Task::execute($taskSub());


$taskPub = function() {
    $topic = "zan_mqworker_test";
    while (true) {
        yield taskSleep(1000);
        try {
            yield SQS::publish($topic, time());
        } catch (\Exception $ex) {
            echo_exception($ex);
        }
    }
};
Task::execute($taskPub());