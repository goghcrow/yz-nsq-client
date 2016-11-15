<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Components\Contract\Nsq\MsgHandler;
use Zan\Framework\Components\Nsq\Utils\Lock;
use Zan\Framework\Contract\Network\Bootable;
use Zan\Framework\Foundation\Core\Config;
use Zan\Framework\Foundation\Coroutine\Task;

class SQS implements Bootable
{
    /**
     * @var Producer[]
     */
    private static $producers = [];

    /**
     * @var Consumer[] map<string, list<Consumers>>
     */
    private static $consumers = [];

    /**
     * @param string $topic
     * @param string $channel
     * @param MsgHandler|callable $msgHandler
     * @param int $maxInFlight
     * @return \Generator yield return Consumer
     * @throws NsqException
     */
    public static function subscribe($topic, $channel, $msgHandler, $maxInFlight = -1)
    {
        Command::checkTopicChannelName($topic);
        Command::checkTopicChannelName($channel);

        if ($msgHandler instanceof MsgHandler) {

        } else if (is_callable($msgHandler)) {
            $msgHandler = new SimpleMsgHandler($msgHandler);
        } else {
            throw new NsqException("invalid msgHandler");
        }

        $consumer = new Consumer($topic, $channel, $msgHandler);
        $maxInFlight = $maxInFlight > 0 ? $maxInFlight : NsqConfig::getMaxInFlightCount();
        $consumer->changeMaxInFlight($maxInFlight ?: $maxInFlight);

        $lookup = NsqConfig::getLookup();
        if (empty($lookup)) {
            throw new NsqException("no nsq lookup address");
        }
        if (is_array($lookup)) {
            yield $consumer->connectToNSQLookupds($lookup);
        } else {
            yield $consumer->connectToNSQLookupd($lookup);
        }

        if (!isset(static::$consumers["$topic:$channel"])) {
            static::$consumers["$topic:$channel"] = [];
        }
        static::$consumers["$topic:$channel"][] = $consumer;
    }

    /**
     * @param string $topic
     * @param string $channel
     * @return bool
     */
    public static function unSubscribe($topic, $channel)
    {
        if (!isset(static::$consumers["$topic:$channel"]) || !static::$consumers["$topic:$channel"]) {
            return false;
        }

        /* @var Consumer $consumer */
        foreach (static::$consumers["$topic:$channel"] as $consumer) {
            $consumer->stop();
        }
        return true;
    }

    /**
     * @param string $topic
     * @param string[] ...$messages
     * @return \Generator yield bool
     * @throws NsqException
     */
    public static function publish($topic, ...$messages)
    {
        Command::checkTopicChannelName($topic);

        $lookup = NsqConfig::getLookup();
        if (empty($lookup)) {
            throw new NsqException("no nsq lookup address");
        }

        if (empty($messages)) {
            throw new NsqException("empty messages");
        }

        foreach ($messages as $i => $message) {
            if (is_scalar($message)) {
                $messages[$i] = strval($message);
            } else {
                $messages[$i] = json_encode($message, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
            }
        }

        yield Lock::lock(__CLASS__);
        try {
            if (!isset(static::$producers[$topic])) {
                yield static::prepareProducers([$topic => NsqConfig::getMaxConnectionPerTopic()]);
            }
        } finally {
            yield Lock::unlock(__CLASS__);
        }


        $producer = static::$producers[$topic];
        if (count($messages) === 1) {
            $resp = (yield $producer->publish($messages[0]));
        } else {
            $resp = (yield $producer->multiPublish($messages));
        }
        if ($resp === "OK") {
            yield true;
        } else {
            $msgStr = implode("//", $messages);
            sys_echo("publish fail, [topic=>$topic, msg=$msgStr, resp=$resp]");
            yield false;
        }
    }

    /**
     * @param array $conf map<string, int> [topic => connNum]
     * @return \Generator
     * @throws NsqException
     */
    private static function prepareProducers(array $conf)
    {
        $lookup = NsqConfig::getLookup();
        if (empty($lookup)) {
            throw new NsqException("no nsq lookup address");
        }

        foreach ($conf as $topic => $connNum) {
            Command::checkTopicChannelName($topic);
            if (isset($producer[$topic])) {
                continue;
            }

            $producer = new Producer($topic, intval($connNum));
            if (is_array($lookup)) {
                yield $producer->connectToNSQLookupds($lookup);
            } else {
                yield $producer->connectToNSQLookupd($lookup);
            }
            static::$producers[$topic] = $producer;
        }
    }

    public static function stat()
    {
        $stat = [
            "consumer" => [],
            "producer" => [],
        ];
        foreach (static::$consumers as $consumer) {
            $stat["consumer"][] = $consumer->stats();
        }
        foreach (static::$producers as $producer) {
            $stat["producer"][] = $producer->stats();
        }
        return $stat;
    }

    public function bootstrap($server)
    {
        NsqConfig::init(Config::get("nsq", []));

        $task = function() {
            try {
                $topics = NsqConfig::getTopic();
                if (empty($topics)) {
                    return;
                }

                $num = NsqConfig::getMaxConnectionPerTopic();
                $values = array_fill(0, count($topics), $num);
                $conf = array_combine($topics, $values);
                yield static::prepareProducers($conf);
            } catch (\Exception $ex) {
                echo_exception($ex);
            }
        };
        
        Task::execute($task());
    }
}