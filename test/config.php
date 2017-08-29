<?php

/**
 * 说明:
 * 1. 只有lookup项必填, 其他全部选填
 * 2. 所有时间配置 单位: ms
 */
return [
    // ["必填"]lookup 节点地址
    "lookup" => [
//        "http://sqs-qa.s.qima-inc.com:4161"
        "http://qabb-qa-nsqtest0:4161"
    ],

    // ["建议填写"] 需要publish的topic列表, 预先配置, 会在workerStart时候建立好连接
    "topic" => [
        //"test_php",
        //"test_php_ext"
    ],

    // ====================================== 以下选择性配置 ====================================
    // ======================================  identity  ======================================
    "identify" => [
        // Identifiers sent to nsqd representing this client
        "client_id" => gethostname(),

        "hostname" => gethostname(),

        "feature_negotiation" => true, // is enable negotiation

        // Duration of time between heartbeats. This must be less than ReadTimeout
        "heartbeat_interval" => 30 * 1000, // default: 30000ms

        // Size of the buffer (in bytes) used by nsqd for buffering writes to this connection
        "output_buffer_size" => 16384, // 16kb

        // Timeout used by nsqd before flushing buffered writes (set to 0 to disable).
        // WARNING: configuring clients with an extremely low
        // (< 25ms) output_buffer_timeout has a significant effect
        // on nsqd CPU usage (particularly with > 50 clients connected).
        "output_buffer_timeout" => -1, // default 250ms

        // "tls_v1" => false,
        "snappy" => false,
        "deflate" => false,
        "deflate_level" => 1,
        "sample_rate" => 0,

        "user_agent" => "zan-nsq-client/v0.2",

        // 服务端消息超时时间
        // The server-side message timeout for messages delivered to this client
        "msg_timeout" => 60000, // ms
    ],



    // ======================================  Lookup  ======================================
    // nsqd连接延迟关闭时间
    "delaying_close_time" => 5 * 1000,

    // 连接nsqd超时时间
    "nsqd_connect_timeout" => 3 * 1000,

    // lookup连接超时时间
    "nsqlookupd_connect_timeout" => 3 * 1000,

    // 通过lookupd更新nsqd节点周期
    // Duration between polling lookupd for new producers, and fractional jitter to add to
    // the lookupd pool loop. this helps evenly distribute requests even if multiple consumers
    // restart at the same time
    "lookupd_poll_interval" => 60 * 1000,
    "lookupd_poll_jitter" => 0.3, // 0~1



    // =====================================  Producer  =====================================
    // 需要publish的topic列表, 预先配置, 会在workerStart时候建立好连接
    /*
    "topic" => [
        "zan_mqworker_test",
    ],
    */

    // publish临时连接生命周期, 要大于消息处理时长
    // "disposable_connection_lifecycle" => 1000,

    // 发布超时时间
    "publish_timeout" => 1000, // 3 * 1000,

    // 发布重试次数
    "publish_retry" => 3,


    // =====================================  Consumer  =====================================

    // 是否开启消息自动回复
    // MsgHandler $result !== false 时, 自动发送FIN命令完成任务
    "message_auto_response" => true,

    // pipe_count: 设置当前消费者实例(多个NSQD连接)最大允许的in-flight消息数量, 每个nsqd连接均分
    // Maximum number of messages to allow in flight (concurrency knob)
    "max_in_flight" => 2, //2500,

    // 每个topic的最大nsqd连接数, 最小值为lookup节点查询当前nsqd数量
    // max(count($nsqdList), $this->maxConnectionNum)
    "max_connection_per_topic" => 50,

    // consumer消息最大重试次数
    "max_attempts" => 5,

    /**
     * 消息requeue backoff设置
     * $delay = Backoff::calculate($msg->getAttempts(), $c["min"], $c["max"], $c["factor"], $c["jitter"]);
     */
    "message_backoff" => [
        "min" => 2 * 1000, // backoff 起始 时间
        // Maximum amount of time to backoff when processing fails
        // 0 == no backoff
        "max" => 1000 * 60 * 10, // backoff 时间 上限
        "factor" => 2,
        "jitter" => 0.3 // 0~1
    ],

    // 在nsqd连接间重新分配max-in-flight的时间间隔
    "rdy_redistribute_interval" => 5 * 1000,

    // 重新分配rdy的闲置等待时间阈值
    // Duration to wait for a message from a producer when in a state where RDY
    // counts are re-distributed (ie. max_in_flight < num_producers)
    "low_rdy_idle_timeout" => 10 * 1000,


    // =====================================  General  =====================================
    // prod机器 /proc/sys/net/core/rmem_max = /proc/sys/net/core/wmem_max = 327679
    "socket_buffer_size" => 327679,
    "packet_size_limit" => 327679,

    // auth 不支持
    // "auto_secret" => "",
];
