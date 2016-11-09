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
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/../../vendor/autoload.php";
NsqConfig::init();
\Zan\Framework\Foundation\Core\Debug::detect();

function subs()
{
    $delegate = new SimpleConnDelegate();
    $conn = new Connection("10.9.6.49", 4150, $delegate);
    yield $conn->connect();
    $conn->writeCmd(Command::subscribe("zan_mqworker_test", "ch2"));
    $conn->writeCmd(Command::ready(10));
}
//Task::execute(subs());


class TestMsgHandler implements MsgHandler
{

    public function handleMessage(Message $message)
    {
        if (rand(1, 10) == 1) {
            sys_echo($message->getId() . "#exception");
            throw new Exception("###########");
        }

        // sys_echo(__METHOD__ . ":" . $message);
        yield true;
    }

    public function logFailedMessage(Message $message)
    {
        sys_echo(__METHOD__ . ":" . $message);
    }
}

function testSub()
{
    try {
        $msgHandler = new TestMsgHandler();
        $consumer = new Consumer("zan_mqworker_test", "ch1", $msgHandler);
        $consumer->changeMaxInFlight(10);
        yield $consumer->connectToNSQLookupd("http://sqs-qa.s.qima-inc.com:4161");

        $producer = new Producer("10.9.52.240", 4150);
        yield $producer->connect();
        while (true) {
            yield taskSleep(200);
            yield $producer->publish("zan_mqworker_test", "hello sqs");
        }
//        yield taskSleep(5000);
//        $consumer->stop();
    } catch (\Exception $ex) {
        echo_exception($ex);
    }
}
Task::execute(testSub());