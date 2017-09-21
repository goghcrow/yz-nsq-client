<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Components\Nsq\Contract\MsgHandler;
use Zan\Framework\Components\Nsq\Utils\Lock;
use Zan\Framework\Utilities\Types\Json;
use ZanPHP\Container\Container;
use ZanPHP\Contracts\ServiceChain\ServiceChainer;

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

        $desiredTag = (yield static::getServiceChainName());
        if (!$desiredTag) {
            $desiredTag = isset($options['desiredTag']) ? strval($options['desiredTag']) : '';
        }

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
     * @param mixed $message
     * @param MessageParam $params
     * @return \Generator yield bool
     * @throws NsqException
     */
    public static function publish($topic, $message, $params = null)
    {
        if (is_scalar($message)) {
            $message = strval($message);
        } else {
            $message = Json::encode($message);
        }
        return self::publishStrings($topic, [$message], $params);
    }


    /**
     * @param string $topic
     * @param mixed[] $messages
     * @param MessageParam $params
     * @return \Generator yield bool
     * @throws NsqException
     */
    public static function publishMulti($topic, $messages, $params = null)
    {
        $messages = array_filter((array) $messages);
        foreach ($messages as $i => $message) {
            if (is_scalar($message)) {
                $messages[$i] = strval($message);
            } else {
                $messages[$i] = Json::encode($message);
            }
        }
        return self::publishStrings($topic, $messages, $params);
    }

    /**
     * @param string $topic
     * @param string[] $messages
     * @param MessageParam $params
     * @return \Generator yield bool
     * @throws NsqException
     */
    private static function publishStrings($topic, $messages, $params = [])
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
        /*
        foreach ($messages as $i => $message) {
            if (is_scalar($message)) {
                $messages[$i] = strval($message);
            } else {
                $messages[$i] = Json::encode($message);
            }
        }
        */
        yield Lock::lock(__CLASS__);
        try {
            if (!isset(InitializeSQS::$producers[$topic])) {
                yield InitializeSQS::initProducers([$topic => NsqConfig::getMaxConnectionPerTopic()]);
            }
        } finally {
            Lock::unlock(__CLASS__);
        }

        $chainName = (yield getContext("service-chain-name"));
        if ($chainName && is_scalar($chainName)) {
             if (!$params instanceof MessageParam) {
                 $params = new MessageParam();
             }
             $params->withTag($chainName);
        }

        $producer = InitializeSQS::$producers[$topic];
        $retry = NsqConfig::getPublishRetry();
        if (empty($params)) {
            yield self::publishWithRetry($producer, $topic, $messages, $params, $retry);
        } else {
            foreach ($messages as $message) {
                yield self::publishWithRetry($producer, $topic, $message, $params, $retry);
            }
        }
    }

    private static function publishWithRetry(Producer $producer, $topic, $messages, $params, $n = 3)
    {
        $resp = null;
        try {
            if (is_array($messages)) {
                $resp = (yield $producer->multiPublish($messages)); // mpub not supports extends
            } else {
                $resp = (yield $producer->publish($messages, $params));
            }
        } catch (\Throwable $ex) {
        } catch (\Exception $ex) {
        }

        if ($resp === "OK") {
            yield true;
        } elseif (substr($resp, 0, 2) == "OK" && strlen($resp) == 30) {
            // resp of pub trace
            // 2 + 8 + 8 + 8 + 4 == 30
            //OK(2-bytes)+[8-byte internal id]+[8-byte trace id from client]+[8-byte internal disk queue offset]+[4 bytes internal disk queue data size]
            yield unpack('H16internal/H16trace/H16offset/H8size', substr($resp, 2));
        } else {
            if (--$n > 0) {
                if ($resp === "E_BAD_TOPIC") {
                    yield $producer->queryNSQLookupd();
                }
                $i = (3 - $n);
                $msg = isset($ex) ? $ex->getMessage() : "";
                sys_error("publish fail <$msg>, retry [topic=$topic, n=$i]");
                yield taskSleep(100 * $i);
                yield self::publishWithRetry($producer, $topic, $messages, $params, $n);
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

    private static function getServiceChainName()
    {
        $chainName = (yield getContext("service-chain-name"));

        if (!$chainName) {
            $container = Container::getInstance();
            if ($container->has(ServiceChainer::class)) {
                $serviceChain = $container->make(ServiceChainer::class);
                $chainValue = $serviceChain->getChainValue(ServiceChainer::TYPE_JOB);
                if ($chainValue) {
                    yield setContext("service-chain-value", $chainValue);
                }

                if (isset($chainValue["name"])) {
                    $chainName = $chainValue["name"];
                    yield setContext("service-chain-name", $chainName);
                }
            }
        }

        yield $chainName;
    }
}
