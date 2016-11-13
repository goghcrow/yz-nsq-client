<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Command;
use Zan\Framework\Components\Nsq\Connection;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";

$task = function() {
    try {
        /* @var Connection $conn */
        $liftCycle = 5000;
        list($conn) = (yield Connection::getDisposable("10.9.80.209", 4150, $liftCycle));
        $conn->writeCmd(Command::nop());
        $resp = (yield $conn->syncWriteCmd(Command::ping()));
        assert($resp === "E_INVALID invalid command [80 73 78 71]");
        var_dump("hello");

        // no resp
        try {
            $resp = (yield $conn->syncWriteCmd(Command::nop(), 1000));
        } catch (\Exception $ex) {  }

        var_dump("world");
    } catch (\Exception $ex) {
        echo_exception($ex);
    }
};
Task::execute($task());