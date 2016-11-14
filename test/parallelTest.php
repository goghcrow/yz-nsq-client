<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Utils\Dns;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Common\HttpClient;

require_once __DIR__ . "/boot.php";

$task = function($i) {
//    yield taskSleep(10 * rand(1, 10));
    yield taskSleep(10 * $i + 1);
    if (rand(0, 1) === 0) {
        throw new \Exception("parallel exception");
    }
//    yield HttpClient::newInstance("www.baidu.com")->get("/");
    yield true;
};

$parallelTask = function() use($task) {
    try {
        $tasks = [];
        for ($i = 0; $i < 10; $i++) {
            $tasks["job_$i"] = $task($i);
        }
        $list = (yield parallel($tasks));
        // fix zan parallel bug
        echo count($list), "\n";
        foreach ($list as $item) {
            if ($item instanceof \Exception) {
                // echo_exception($item);
                echo $item->getMessage(), "\n";
            }
        }
        echo "done\n";
    } catch (\Exception $ex) {
        echo_exception($ex);
    }
};

Task::execute($parallelTask());