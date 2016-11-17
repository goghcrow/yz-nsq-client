<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Producer;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";

ini_set("memory_limit", "1024m");
cli_set_process_title(__FILE__);

$taskPub = function () {
    $payload = str_repeat("a", 1024 * 2);

    $task = function() use($payload) {
        try {
            $topic = "zan_mqworker_test";
            /* @var Producer $producer */
            while (true) {
                yield SQS::publish($topic, $payload);
                yield taskSleep(500);
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

Task::execute($taskPub());