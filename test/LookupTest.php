<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Connection;
use Zan\Framework\Components\Nsq\Lookup;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/boot.php";


$connTasl = function() {
    $topic = "zan_mqworker_test";
    $lookupDevUrl = "http://nsq-dev.s.qima-inc.com:4161";
    $lookupQaUrl = "http://sqs-qa.s.qima-inc.com:4161";
    $maxConnNum = 2;

    $lookup = new Lookup($topic, $maxConnNum);


    /* @var Connection[] $conns */
    $conns = (yield $lookup->connectToNSQD("10.9.52.240", 4150));
    assert(count($conns) === $maxConnNum);

    yield $lookup->connectToNSQLookupd($lookupDevUrl);
    yield $lookup->connectToNSQLookupd($lookupQaUrl);
};

$task = function() {
    $topic = "zan_mqworker_test";
    $lookupDevUrl = "http://nsq-dev.s.qima-inc.com:4161";
    $lookupQaUrl = "http://sqs-qa.s.qima-inc.com:4161";
    $maxConnNum = 1;

    $lookup = new Lookup($topic, $maxConnNum);

    /* @var Connection[] $conns */
    $conns = (yield $lookup->connectToNSQD("10.9.52.240", 4150));
    assert(count($conns) === $maxConnNum);
    $lookup->disconnectFromNSQDConn($conns[0]);
    yield $lookup->reconnect($conns[0]);

    $conns = (yield $lookup->connectToNSQLookupd($lookupDevUrl));
    $conns = (yield $lookup->connectToNSQLookupd($lookupQaUrl));
    $lookup->disconnectFromNSQLookupd($lookupQaUrl);
};

Task::execute($task());