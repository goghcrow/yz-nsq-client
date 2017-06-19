<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\NsqConfig;
use Zan\Framework\Components\Nsq\Producer;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Contract\Async;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Common\HttpClient;
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/boot.php";


$parallelTask = function() {
    $topic = "zan_mqworker_test";
//    (new SQS())->bootstrap(null);

    $task = function() {
        $topic = "zan_mqworker_test";
        yield SQS::publish($topic, "hello");
        yield SQS::publish($topic, "hello", "world");

    };

    $tasks = [];
    for($i = 0; $i < 10; $i++) {
        $tasks[$i] = $task();
    }

    yield parallel($tasks);
    swoole_timer_after(6000, function() {
        print_r(SQS::stat());
    });
};
// Task::execute($parallelTask());
//swoole_timer_tick(2000, function() use($parallelTask) {
//    Task::execute($parallelTask());
//});


$parallelTask = function() {
    $task = function() {
        try {
            $lookupQaUrl = "http://sqs-qa.s.qima-inc.com:4161";
            $topic = "zan_mqworker_test";
            $maxConnectionNum = 5;
            $producer = new Producer($topic, $maxConnectionNum);
            yield $producer->connectToNSQLookupd($lookupQaUrl);

            $ret1 = (yield $producer->publish("hello sqs"));
            $ret2 = (yield $producer->multiPublish(["hello", "sqs"]));
            yield [$ret1, $ret2];

        } catch (\Exception $ex) {
            echo_exception($ex);
        }
    };

    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = $task();
    }
    $list = (yield parallel($tasks));
    print_r($list);
};

//Task::execute($parallelTask());


$parallelTask = function() {
    $task = function(Producer $producer) {
        try {

            $ret1 = (yield $producer->publish("hello sqs"));
            $ret2 = (yield $producer->multiPublish(["hello", "sqs"]));
            yield [$ret1, $ret2];

        } catch (\Exception $ex) {
            echo_exception($ex);
        }
    };


    $lookupQaUrl = "http://sqs-qa.s.qima-inc.com:4161";
    $topic = "zan_mqworker_test";
    $maxConnectionNum = 5;
    $producer = new Producer($topic, $maxConnectionNum);
    yield $producer->connectToNSQLookupd($lookupQaUrl);


    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = $task($producer);
    }

    $list = (yield parallel($tasks));
    print_r($list);
};
//Task::execute($parallelTask());


$parallelTask = function() {
    $task = function($i) {
        try {
            $topic = "zan_mqworker_test";
            $ret1 = (yield SQS::publish($topic, "hello sqs"));
            // yield taskSleep(2000);
            $ret2 = (yield SQS::publish($topic, "hello sqs", "hello"));
            yield [$ret1, $ret2];
        } catch (\Exception $ex) {
            echo_exception($ex);
        }
    };

    $tasks = [];
    for ($i = 0; $i < 4; $i++) {
        $tasks[] = $task($i);
    }
    $list = (yield parallel($tasks));

    print_r($list);
};

Task::execute($parallelTask());



//class AsyncTimer implements Async
//{
//    private $callback;
//
//    public static function after($interval, callable $callback)
//    {
//        // yield taskSleep(1000);
//        // yield $callback();
//        $self = new static;
//        Timer::after($interval, function() use($self, $callback) {
//            $callback();
//            call_user_func($self->callback, null, null);
//        });
//        yield $self;
//    }
//
//    public function execute(callable $callback, $task)
//    {
//        $this->callback = $callback;
//    }
//}
//$t = function() {
//    sys_echo("1");
//    yield AsyncTimer::after(1000, function() {
//        sys_echo("2");
//    });
//};
//Task::execute($t());

