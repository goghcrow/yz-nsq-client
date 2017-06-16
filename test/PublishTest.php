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
    try {
        $ok = (yield SQS::publish($topic, $oneMsg));
        var_dump($ok);
    } catch (\Throwable $t) {
        echo_exception($t);
    } catch (\Exception $e) {
        echo_exception($e);
    }

    try {
        $ok = (yield SQS::publish($topic, "hello", "hi"));
        var_dump($ok);
    } catch (\Throwable $t) {
        echo_exception($t);
    } catch (\Exception $e) {
        echo_exception($e);
    }

    try {
        $ok = (yield SQS::publish($topic, ...$multiMsgs));
        var_dump($ok);
    } catch (\Throwable $t) {
        echo_exception($t);
    } catch (\Exception $e) {
        echo_exception($e);
    }


    swoole_event_exit();
}

Task::execute(taskPub());