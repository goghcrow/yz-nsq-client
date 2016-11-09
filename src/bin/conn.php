<?php

use Zan\Framework\Components\Nsq\Command;
use Zan\Framework\Components\Nsq\ConnDelegate;
use Zan\Framework\Components\Nsq\Connection;
use Zan\Framework\Components\Nsq\Consumer;
use Zan\Framework\Components\Nsq\Message;
use Zan\Framework\Components\Nsq\MsgHandler;
use Zan\Framework\Components\Nsq\NsqConfig;
use Zan\Framework\Components\Nsq\NsqException;
use Zan\Framework\Components\Nsq\Producer;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/../../vendor/autoload.php";

NsqConfig::init();
\Zan\Framework\Foundation\Core\Debug::detect();

function testConn()
{
    try {
        $delegate = new SimpleConnDelegate();
        $conn = new Connection("10.9.6.49", 4150, $delegate);
        yield $conn->connect();
    } catch (\Exception $ex) {
        echo_exception($ex);
    }
}
Task::execute(testConn());