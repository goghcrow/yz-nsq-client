<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";

$task = function() {
    yield taskSleep(10 * rand(1, 10));
    if (rand(0, 1) === 0) {
        throw new \Exception("parallel exception");
    }
    yield true;
};

$parallelTask = function() use($task) {
    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks["job_$i"] = $task();
    }
    $list = (yield parallel($tasks));
    // fix zan parallel bug
    var_dump($list);
};

Task::execute($parallelTask());