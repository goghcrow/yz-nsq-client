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


class SimpleConnDelegate implements ConnDelegate
{
    public function onResponse(Connection $conn, $bytes){}
    public function onError(Connection $conn, $bytes){sys_echo(__METHOD__ . ":$bytes");}
    public function onMessage(Connection $conn, Message $msg){
        $conn->writeCmd(Command::finish($msg));
    }
    public function onMessageFinished(Connection $conn, Message $msg){}
    public function onMessageRequeued(Connection $conn, Message $msg){}
    public function onBackoff(Connection $conn){}
    public function onContinue(Connection $conn){}
    public function onResume(Connection $conn){}
    public function onIOError(Connection $conn, \Exception $ex){echo_exception($ex);}
    public function onHeartbeat(Connection $conn){sys_echo(__FUNCTION__);}
    public function onClose(Connection $conn){sys_echo(__METHOD__);}
    public function onReceive(Connection $conn, $bytes){sys_echo("recv:" . str_replace("\n", "\\n", $bytes));}
    public function onSend(Connection $conn, $bytes){sys_echo("send:" . str_replace("\n", "\\n", $bytes));}
}

function testPub()
{
    try {
        $producer = new Producer("10.9.6.49", "port");
        // yield $producer->ping();

        while (true) {
            $resp = (yield $producer->publish("zan_mqworker_test", "hi"));
            var_dump($resp);

            yield taskSleep(1000);

            $resp = (yield $producer->multiPublish("zan_mqworker_test", range(1, 2)));
            var_dump($resp);
        }
    } catch (\Exception $ex) {
        echo_exception($ex);
    }
}
//Task::execute(testPub());