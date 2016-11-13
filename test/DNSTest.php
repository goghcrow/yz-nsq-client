<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Dns;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";


$task = function() {
    // success
    $ip = (yield Dns::lookup("sqs-qa.s.qima-inc.com"));
    assert(filter_var($ip, FILTER_VALIDATE_IP) === $ip);

    // fail
    try {
        yield Dns::lookup("xxx.yyy.xxx");
        assert(false);
    } catch (\Exception $ex) {
        assert($ex);
    }

    swoole_event_exit();
};

Task::execute($task());

