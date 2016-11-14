<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Contract\Nsq\MsgHandler;
use Zan\Framework\Components\Nsq\Consumer;
use Zan\Framework\Components\Nsq\Message;
use Zan\Framework\Components\Nsq\SQS;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/boot.php";


class TestMsgHandler implements MsgHandler
{

    public function handleMessage(Message $message, Consumer $consumer)
    {
    }

    public function logFailedMessage(Message $message, Consumer $consumer)
    {
    }
}

$task = function()
{
    $topic = "zan_mqworker_test";
    $ch = "ch1";

    /* @var Consumer $consumer */
    $consumer = (yield SQS::subscribe($topic, $ch, function(Message $msg, Consumer $consumer) {
//         print_r($consumer->stats());
        // var_dump($consumer->isStarved());


        echo $msg, "\n";
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


    // 在backoff期间如何close ?!
    // TODO write "FIN xxx fail, because connection is closing

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


Task::execute($task());