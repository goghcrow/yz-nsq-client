<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Components\Nsq\Contract\MsgHandler;
use Zan\Framework\Components\Nsq\Utils\Lock;
use Zan\Framework\Utilities\Types\Json;

class SQS
{
    /**
     * @param string $topic
     * @param string $channel
     * @param MsgHandler|callable $msgHandler
     * @param array $options can contains 'maxInFlight' 'desiredTag'
     * @return \Generator yield return Consumer
     * @throws NsqException
     */
    public static function subscribe($topic, $channel, $msgHandler, $options = [])
    {
        Command::checkTopicChannelName($topic);
        Command::checkTopicChannelName($channel);

        if (is_callable($msgHandler)) {
            $msgHandler = new SimpleMsgHandler($msgHandler);
        }

        if (!($msgHandler instanceof MsgHandler)) {
            throw new NsqException("invalid msgHandler");
        }

        $consumer = new Consumer($topic, $channel, $msgHandler);
        
        $maxInFlight = isset($options['maxInFlight']) ? intval($options['maxInFlight']) : -1;
        $maxInFlight = $maxInFlight > 0 ? $maxInFlight : NsqConfig::getMaxInFlightCount();
        $consumer->changeMaxInFlight($maxInFlight ?: $maxInFlight);

        $desiredTag = isset($options['desiredTag']) ? strval($options['desiredTag']) : '';
        $consumer->setDesiredTag($desiredTag);
        
        $lookup = NsqConfig::getLookup();
        if (empty($lookup)) {
            throw new NsqException("no nsq lookup address");
        }

        if (!isset(InitializeSQS::$consumers["$topic:$channel"])) {
            InitializeSQS::$consumers["$topic:$channel"] = [];
        }
        InitializeSQS::$consumers["$topic:$channel"][] = $consumer;

        if (!is_array($lookup)) {
            $lookup = [$lookup];
        }
        yield $consumer->connectToNSQLookupds($lookup);
    }

    /**
     * @param string $topic
     * @param string $channel
     * @return bool
     */
    public static function unSubscribe($topic, $channel)
    {
        if (!isset(InitializeSQS::$consumers["$topic:$channel"]) || !InitializeSQS::$consumers["$topic:$channel"]) {
            return false;
        }

        /* @var Consumer $consumer */
        foreach (InitializeSQS::$consumers["$topic:$channel"] as $consumer) {
            $consumer->stop();
        }
        return true;
    }

    public static function params()
    {
        return new MessageParam();
    }

    /**
     * @param string $topic
     * @param string[] $messages
     * @param MessageParam $params
     * @return \Generator yield bool
     * @throws NsqException
     */
    public static function publish($topic, $messages, $params = null)
    {
        Command::checkTopicChannelName($topic);

        $lookup = NsqConfig::getLookup();
        if (empty($lookup)) {
            throw new NsqException("no nsq lookup address");
        }
        $messages = (array) $messages;
        if (empty($messages)) {
            throw new NsqException("empty messages");
        }
        foreach ($messages as $i => $message) {
            if (is_scalar($message)) {
                $messages[$i] = /*strval(*/$message/*)*/;
            } else {
                $messages[$i] = Json::encode($message);
            }
        }

        yield Lock::lock(__CLASS__);
        try {
            if (!isset(InitializeSQS::$producers[$topic])) {
                yield InitializeSQS::initProducers([$topic => NsqConfig::getMaxConnectionPerTopic()]);
            }
        } finally {
            yield Lock::unlock(__CLASS__);
        }

        $producer = InitializeSQS::$producers[$topic];
        $retry = NsqConfig::getPublishRetry();
        yield self::publishWithRetry($producer, $topic, $messages, $params, $retry);
    }

    private static function publishWithRetry(Producer $producer, $topic, $messages, $params, $n = 3)
    {
        $resp = null;

        try {
            if (count($messages) === 1) {
                $resp = (yield $producer->publish($messages[0], $params));
            } else {
                $resp = (yield $producer->multiPublish($messages, $params));
            }
        } catch (\Throwable $ex) {
        } catch (\Exception $ex) {
        }

        if ($resp === "OK") {
            yield true;
        } else {
            if (--$n > 0) {
                if ($resp === "E_BAD_TOPIC") {
                    yield $producer->queryNSQLookupd();
                }
                $i = (3 - $n);
                $msg = isset($ex) ? $ex->getMessage() : "";
                sys_error("publish fail <$msg>, retry [topic=$topic, n=$i]");
                yield taskSleep(100 * $i);
                yield self::publishWithRetry($producer, $topic, $messages, $n);
            } else {
                $previous = isset($ex) ? $ex : null;
                throw  new NsqException("publish [$topic] fail", 0, $previous, [
                    "topic" => $topic,
                    "msg"   => $messages,
                    "resp"  => $resp,
                ]);
            }
        }
    }

    /**
     * @return array
     */
    public static function stat()
    {
        $stat = [
            "consumer" => [],
            "producer" => [],
        ];
        foreach (InitializeSQS::$consumers as $topicCh => $consumers) {
            /* @var Consumer $consumer */
            foreach ($consumers as $consumer) {
                $stat["consumer"][$topicCh] = $consumer->stats();
            }
        }
        foreach (InitializeSQS::$producers as $topic => $producer) {
            $stat["producer"][$topic] = $producer->stats();
        }
        return $stat;
    }
}
