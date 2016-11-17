<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Consumer;
use Zan\Framework\Components\Nsq\Contract\MsgHandler;
use Zan\Framework\Components\Nsq\Message;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";

class BenchMsgHandler2 implements MsgHandler
{

    public function handleMessage(Message $message, Consumer $consumer)
    {
        yield taskSleep(100);
    }

    public function logFailedMessage(Message $message, Consumer $consumer)
    {
        sys_echo("error: logFailedMessage " . $message);
    }
}

ini_set("memory_limit", "1024m");
cli_set_process_title(__FUNCTION__);

$task = function()
{
    $topic = "zan_mqworker_test";
    $ch = "ch1";
    /* @var Consumer $consumer */
    $consumer = (yield SQS::subscribe($topic, $ch, new BenchMsgHandler2(), 1));
};

//swoole_timer_tick(1000, function() {
//    print_r(SQS::stat());
//    echo number_format(memory_get_usage()), "byte\n";
//    echo number_format(memory_get_usage(true)), "byte\n";
//});

Task::execute($task());