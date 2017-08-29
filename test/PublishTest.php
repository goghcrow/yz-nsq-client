<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Producer;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";


function taskPub()
{
//    $topic = "zan_mqworker_test";
//    $topic = "test_php_sdk_ext";
    //$topic = "test_php_ext";
    $topic = "test_php";

    $oneMsg = "hello世界";
    $multiMsgs = [
        "hello",
        "hi",
    ];


    /* @var Producer $producer */
for (;;) {
    try {
        $ok = (yield SQS::publish($topic, $oneMsg));
        var_dump($ok);

        $ok = (yield SQS::publish($topic, [$oneMsg,$oneMsg]));
        var_dump($ok);

        $ok = (yield SQS::publish($topic, $oneMsg, SQS::params()));
        var_dump($ok);

        $ok = (yield SQS::publish($topic, [$oneMsg,$oneMsg], SQS::params()));
        var_dump($ok);
/*
        $ok = (yield SQS::publish($topic, $oneMsg, SQS::params()->withTag('TestTag')));
        var_dump($ok);

        $ok = (yield SQS::publish($topic, [$oneMsg,$oneMsg], SQS::params()->withTag('TestTag')));
        var_dump($ok);
*/
        $ok = (yield SQS::publishMulti($topic, $multiMsgs));
        var_dump($ok);

        $ok = (yield SQS::publishMulti($topic, $multiMsgs, SQS::params()));
        var_dump($ok);

        echo "next\n";
/*
        $ok = (yield SQS::publishMulti($topic, $oneMsg, SQS::params()->withTag('TestTag')));
        var_dump($ok);

        $ok = (yield SQS::publish($topic, [$oneMsg,$oneMsg], SQS::params()->withTag('TestTag')));
        var_dump($ok);
*/


    } catch (\Throwable $t) {
        echo_exception($t);
    } catch (\Exception $e) {
        echo_exception($e);
    }
    yield taskSleep(100);
}

/*
    try {
        $ok = (yield SQS::publish($topic, "hello", "hi"));
        var_dump($ok);
    } catch (\Throwable $t) {
        echo_exception($t);
    } catch (\Exception $e) {
        echo_exception($e);
    }

    try {
        $ok = (yield SQS::publish($topic, $multiMsgs));
        var_dump($ok);
    } catch (\Throwable $t) {
        echo_exception($t);
    } catch (\Exception $e) {
        echo_exception($e);
    }
*/

    swoole_event_exit();
}

Task::execute(taskPub());
