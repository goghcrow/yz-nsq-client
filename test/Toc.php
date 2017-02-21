<?php
/**
 * Created by IntelliJ IDEA.
 * User: chuxiaofeng
 * Date: 17/2/21
 * Time: 上午11:34
 */

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\TOC;
use Zan\Framework\Components\Nsq\Toc\MessageBuilder;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/boot.php";


function testToc()
{
    $builder = new MessageBuilder();
//    $builder->taskType("dev_mocktask")
    //TOC分配给各业务的taskType,taskType一定要先注册再使用
    $msg = $builder->taskType("test")->bizId(1)->endTime(10)->addOpt()->build();
    yield TOC::sendCommand($msg);
}

Task::execute(testToc());