<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Producer;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";

$taskPub = function () {
    $payload = str_repeat("a", 1024 * 2);

    $task = function() use($payload) {
        try {
            $topic = "zan_mqworker_test";
            /* @var Producer $producer */
            while (true) {
                yield SQS::publish($topic, $payload);
            }
        } catch (\Exception $ex) {
            echo_exception($ex);
        }
    };

    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = $task();
    }

    yield parallel($tasks);
};

swoole_timer_tick(1000, function() { print_r(SQS::stat()); });

Task::execute($taskPub());