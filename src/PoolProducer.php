<?php

namespace Zan\Framework\Components\Nsq;

use SplObjectStorage;


class PoolProducer
{
    private $lookup;

    /**
     * @var array map<string, list<Producer>>
     */
    private $producers;

    // TODO 连接池相关问题
    /**
     * @var array map<string, list<Producer>>
     */
    private $pendingProducers;

    /**
     * @var SplObjectStorage map<Producer, string>
     */
    private $topicChannels;

    public function __construct(Lookup $lookup)
    {
        $this->lookup = $lookup;
        $this->pendingProducers = new SplObjectStorage();
        $this->topicChannels = new SplObjectStorage();
    }

    public function publish($topic, $body)
    {
        /* @var Producer $producer */
        list($producer) = (yield $this->getProducer($topic));
        yield $producer->publish($topic, $body);
    }

    public function multiPublish($topic, array $messages)
    {
        /* @var Producer $producer */
        list($producer) = (yield $this->getProducer($topic));
        yield $producer->multiPublish($topic, $messages);
    }

    private function querysTopics(...$topics)
    {
        $max = NsqConfig::getMaxConnectionPerNsqd();
        foreach ($topics as $topic) {
            $list = (yield $this->lookup->queryNsqd($topic));
            foreach ($list as list($host, $port)) {
                for ($i = 0; $i < $max; $i++) {
                    $producer = new Producer($host, $port);
                    yield $producer->connect();
                    $this->producers[$topic][] = $producer;
                    $this->topicChannels->attach($producer, $topic);
                }
            }
        }
    }

    private function getProducer($topic)
    {
        /* @var Producer $producer */
        if (!isset($this->producers[$topic])) {
            yield $this->querysTopics($topic);
        }
        $i = array_rand($this->producers[$topic]);
        $producer = $this->producers[$topic][$i];
        // producer is async
        yield [ $producer ];
    }
}