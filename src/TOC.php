<?php
/**
 * Created by IntelliJ IDEA.
 * User: chuxiaofeng
 * Date: 17/2/17
 * Time: 下午6:29
 */

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Components\Nsq\Toc\TocException;
use Zan\Framework\Foundation\Core\Config;
use Zan\Framework\Utilities\Types\Json;

class TOC
{
    /**
     * 默认接受消息的topic
     * @var string
     */
    private static $receiveTopic;

    /***
     * 推送命令至toc
     * 1. 需要先配置nsqlookup地址
     * 2. 需要申请taskType
     * doc: http://gitlab.qima-inc.com/trade-platform/toc/wikis/quick-start
     *
     * TOC接入请联系:liwenjia or chengchao
     * @link http://gitlab.qima-inc.com/trade-platform/toc/wikis/home
     * @param $package
     * @param string|null $topic
     * @return \Generator
     * @throws TocException
     */
    public static function sendCommand($package, $topic = null)
    {
        if ($topic === null) {
            if (self::$receiveTopic === null) {
                self::$receiveTopic = Config::get("zan_toc.topic", "toc_web");
            }
            $topic = self::$receiveTopic;
        }
        $packageStr = Json::encode($package);

        try {
            yield SQS::publish($topic, $package);
        } catch (NsqException $ex) {
            throw new TocException("推送任务至TOC异常.message=$packageStr to topic $topic", 0, $ex);
        }
    }
}