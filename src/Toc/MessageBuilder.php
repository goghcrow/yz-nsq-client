<?php
namespace Zan\Framework\Components\Nsq\Toc;

/**
 * 用以构建TOC能够接受的消息
 * Created by PhpStorm.
 * User: liwenjia@youzan.com
 * Date: 2017/2/6
 * Time: 15:17
 */
class MessageBuilder
{

    private $bizID;
    private $bizExtraID;
    private $taskType;
    private $shardingID;
    private $endTime;
    private $suspendTime;
    private $resumeTime;
    private $opt;
    private $ver = 'PHP_ZAN_1.0.0';

    private $avaliableOptions = ['TASK_ADD', 'TASK_SUSPEND', 'TASK_RESUME', 'TASK_CANCEL', 'TASK_POSTPONE'];

    public function bizId($bizId)
    {
        $this->bizID = $bizId;
        return $this;
    }

    public function bizExtraID($bizExtraID)
    {
        $this->bizExtraID = $bizExtraID;
        return $this;
    }

    public function taskType($taskType)
    {
        $this->taskType = $taskType;
        return $this;
    }

    /***
     * 任务到期时间
     * 单位为秒的长整
     * @param $endTime
     * @return $this
     */
    public function endTime($endTime)
    {
        if(strlen($endTime) === 10){
            $endTime = $endTime * 1000;
        }
        $this->endTime = $endTime;
        return $this;
    }

    /***
     * 新增操作
     * @return $this
     */
    public function addOpt(){
        $this->opt = 'TASK_ADD';
        return $this;
    }

    public function build()
    {
        $body = [
            'bizID' => $this->bizID,
            'taskType' => $this->taskType,
            'taskOption' => $this->opt,
            'delayEndTime' => $this->endTime,
        ];
        $this->check($body);

        return $body;
    }

    public function check($body)
    {
        $requireFields = ['taskType','bizID','taskOption'];
        foreach ($requireFields as $field) {
            if(empty($body[$field])){
                throw new TocException("empty toc message field $field");
            }
        }
        if(!in_array($body['taskOption'], $this->avaliableOptions, true)){
            throw new TocException("invalid toc task option");
        }
    }
}