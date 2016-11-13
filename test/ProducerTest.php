<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Dns;
use Zan\Framework\Components\Nsq\Producer;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Contract\Async;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Common\HttpClient;
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/boot.php";


//$task = function() {
//
//    $tasks = [];
//    for ($i = 0; $i < 5; $i++) {
//        $tasks[] = Dns::lookup("www.baidu.com");
//    }
//
//    $ret = (yield parallel($tasks));
//
//    echo "xxxx\n";
//    var_dump($ret);
//};
//Task::execute($task());



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


$parallelTask = function() {
    $topic = "zan_mqworker_test";
//    (new SQS())->bootstrap(null);
//    yield SQS::prepareProducers([
//        $topic => 20,
//    ]);

    $task = function() {
        $topic = "zan_mqworker_test";
        yield SQS::publish($topic, "hello");
        yield SQS::publish($topic, "hello", "world");

    };

    for($i = 0; $i < 10; $i++) {
        Task::execute($task());
    }
};

Task::execute($parallelTask());


//$task = function() {
//    try {
//        $lookupQaUrl = "http://sqs-qa.s.qima-inc.com:4161";
//        $topic = "zan_mqworker_test";
//        $maxConnectionNum = 5;
//        $producer = new Producer($topic, $maxConnectionNum);
//        yield $producer->connectToNSQLookupd($lookupQaUrl);
//        $ret = (yield $producer->publish("hello sqs"));
//        echo "xxxxxxxxxxxxxxxxxxxxxx\n";
//        var_dump($ret);
//
//        $ret = (yield $producer->multiPublish(["hello", "sqs"]));
//        echo "xxxxxxxxxxxxxxxxxxxxxx\n";
//        var_dump($ret);
//    } catch (\Exception $ex) {
//        echo_exception($ex);
//    }
//};
//
//for($i = 0; $i < 10; $i++) {
//    Task::execute($task());
//}