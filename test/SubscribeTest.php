<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Consumer;
use Zan\Framework\Components\Nsq\Contract\MsgHandler;
use Zan\Framework\Components\Nsq\Message;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/boot.php";


class TestMsgHandler implements MsgHandler
{

    public function handleMessage(Message $message, Consumer $consumer)
    {
        echo $message->getId(), "\n";
        echo $message->getBody(), "\n";
        yield taskSleep(1000);
    }

    public function logFailedMessage(Message $message, Consumer $consumer)
    {
        echo "fail: ", $message->getId(), "\n";
    }
}

// auto response + msgHandlerCallback
$task1 = function() {
    $topic = "zan_mqworker_test";
    $ch = "ch1";


    /* @var Consumer $consumer */
    $consumer = (yield SQS::subscribe($topic, $ch, function(Message $msg, Consumer $consumer) {
        echo "recv: " . $msg->getBody(), "\n";
        // echo $msg->getId(), "\n";
        yield taskSleep(1000);
    }));
    swoole_timer_after(3000, function() use($consumer) {
        $consumer->stop();
        // swoole_event_exit();
    });
};
// Task::execute($task1());



// auto response + TestMsgHandlerImpl
$task2 = function() {
    $topic = "zan_mqworker_test";
    $ch = "ch1";
    $msgHandler = new TestMsgHandler();
    yield SQS::subscribe($topic, $ch, $msgHandler);
};

// Task::execute($task2());



$task2 = function() {
    $topic = "zan_mqworker_test";
    $ch = "ch1";
    yield SQS::subscribe($topic, $ch, function(Message $msg) {
        $msg->disableAutoResponse();
        $msg->finish();
        $msg->touch();
        // $msg->requeue($delay, $isBackoff);
    });;
};


$task = function()
{
    $topic = "zan_mqworker_test";
    $ch = "ch1";

    /* @var Consumer $consumer */
    $consumer = (yield SQS::subscribe($topic, $ch, function(Message $msg, Consumer $consumer) {
        // print_r($consumer->stats());
        // var_dump($consumer->isStarved());


        yield taskSleep(1000);
        $consumer->changeMaxInFlight(10);

        $msg->disableAutoResponse();

        if (rand(0, 20) === 0) {
            // throw new \Exception("xx");
            $msg->requeue(1000);
            return;
        }


        $msg->finish();
        // yield true;
    }, 1));


    // core
    Timer::after(5000, function() use($consumer) {
        $consumer->stop();
        Timer::after(5000, function() use($consumer) {
            print_r($consumer->stats());
        });
    });

//    Timer::after(5000, function() use($consumer, $topic, $ch) {
//        SQS::unSubscribe($topic, $ch);
//        Timer::after(5000, function() use($consumer) {
//            print_r($consumer->stats());
//        });
//    });
};

