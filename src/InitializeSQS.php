<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Components\Nsq\Utils\Binary;
use Zan\Framework\Components\Nsq\Utils\MemoryBuffer;
use Zan\Framework\Components\Nsq\Utils\ObjectPool;
use Zan\Framework\Components\Nsq\Utils\StringBuffer;
use Zan\Framework\Contract\Network\Bootable;
use Zan\Framework\Foundation\Core\Config;
use Zan\Framework\Foundation\Coroutine\Task;

class InitializeSQS implements Bootable
{
    /**
     * @var Producer[]
     */
    public static $producers = [];

    /**
     * @var Consumer[] map<string, list<Consumers>>
     */
    public static $consumers = [];

    public function bootstrap($server)
    {
        ObjectPool::create(new Binary(new StringBuffer(8192)), 3000);

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
                yield static::initProducers($conf);
            } catch (\Exception $ex) {
                echo_exception($ex);
            }
        };

        Task::execute($task());
    }

    /**
     * @param array $conf map<string, int> [topic => connNum]
     * @return \Generator
     * @throws NsqException
     */
    public static function initProducers(array $conf)
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

}