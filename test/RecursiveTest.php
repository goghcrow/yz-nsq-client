<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/boot.php";


function recursive() {
    echo memory_get_usage(), "\n";
    echo memory_get_peak_usage(), "\n";
    Timer::after(1, function() {
        recursive();
    });
}
// recursive();
// 内存不会增长


// 会造成scheduler的stack不断增长, 最终stackoverflow
function recursiveGen() {
    echo memory_get_usage(), "\n";
    echo memory_get_peak_usage(), "\n";
    yield taskSleep(1);
    yield recursiveGen();
};
Task::execute(recursiveGen());
// 内存只增长不会降低