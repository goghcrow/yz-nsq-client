<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Command;
use Zan\Framework\Components\Nsq\Connection;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/boot.php";

$task = function() {
    try {
        $conn = new Connection("10.9.6.49", 4150);
        yield $conn->connect();
        $topic = "zan_mqworker_test";
        $chan = "ch1";
        $conn->writeCmd(Command::publish($topic, time()));
        $conn->writeCmd(Command::subscribe($topic, $chan));

        $n = 2;
        $conn->writeCmd(Command::ready($n));
        Timer::after(2000, function() use($conn, $n) {
            assert($conn->getMsgInFlight() === $n);
            assert($conn->isClosing() === false);
            $conn->tryClose();
            assert($conn->isClosing());
        });
    } catch (\Exception $ex) {
        echo_exception($ex);
    } finally {
        // swoole_event_exit();
    }
};

//Task::execute($task());


$task = function() {
    $liftCycle = 2000;
    /* @var Connection $conn */
    list($conn) = (yield Connection::getDisposable("10.9.6.49", 4150, $liftCycle));
    assert($conn->isDisposable());
    Timer::after(3000, function() use($conn) {
        assert($conn->isClosing());
    });
};

Task::execute($task());