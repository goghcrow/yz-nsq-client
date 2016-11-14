<?php

// 只有lookup项必填, 其他全部选填
// 时间单位: ms
return [
    // ["必填"]lookup 节点地址
    "lookup" => [
        "http://sqs-qa.s.qima-inc.com:4161"
    ],

    // 需要publish的topic列表
    "topic" => [
        "zan_mqworker_test",
    ],

    // 发布超时时间
    "publish_timeout" => 1000000, // 3 * 1000, // 废弃 ?! TODO

    // 每个topic的最大nsqd连接数, 最小值为lookup节点查询当前nsqd数量
    // max(count($nsqdList), $this->maxConnectionNum)
    "max_connection_per_topic" => 1, // TODO

    // publish临时连接生命周期
    "disposable_connection_lifecycle" => 100000, // 3 * 1000,

    // prod机器 /proc/sys/net/core/rmem_max = /proc/sys/net/core/wmem_max = 327679
    "socket_buffer_size" => 327679,
    "packet_size_limit" => 327679,

    // nsqd连接延迟关闭时间
    "delaying_close_time" => 5000, // ms
    // 连接nsqd超时时间
    "nsqd_connect_timeout" => 100000, // 1000, // ms //TODO
    // lookup连接超时时间
    "nsqlookupd_connect_timeout" => 3000, // ms

    "identity" => [
        // Identifiers sent to nsqd representing this client
        "client_id" => gethostname(),
        // "hostname" => gethostname(),
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
        // "user_agent" => "",
        // The server-side message timeout for messages delivered to this client
        "msg_timeout" => 60000, // ms
    ],

    // Duration between polling lookupd for new producers, and fractional jitter to add to
    // the lookupd pool loop. this helps evenly distribute requests even if multiple consumers
    // restart at the same time
    "lookupd_poll_interval" => 20*1000,// 60 * 1000, //60s TODO 通过lookupd更新nsqd节点周期
    "lookupd_poll_jitter" => 0.3,

    // Maximum number of messages to allow in flight (concurrency knob)
    "max_in_flight" => 10, //2500, // pipe_count

    // Maximum duration when REQueueing (for doubling of deferred requeue)
    "max_requeue_delay" => 60, //s TODO
    "default_requeue_delay" => 90, //s TODO

    // 是否开启backoff, 发生requeue或处理异常触发backoff
    "enable_backoff" => false, // TODO

    "max_backoff_duration" => 10 * 60 * 1000, // 10min

    "message_backoff" => [
        "min" => 2000, // s // TODO 选择一个合适的min , 跟attemp有关
        // Maximum amount of time to backoff when processing fails 0 == no backoff
        "max" => 1000 * 60 * 10, // 10 min
        "factor" => 2,
        "jitter" => 0.3 // 0~1
    ],

    // Duration to wait for a message from a producer when in a state where RDY
    // counts are re-distributed (ie. max_in_flight < num_producers)
    "low_rdy_idle_timeout" => 10 * 1000, // 10s

    // 在nsqd连接间重新分配max-in-flight的时间间隔
    "rdy_redistribute_interval" => 5 * 1000, //5s

    // consumer消息最大重试次数
    "max_attempts" => 5,

    // auth 不支持
    "auto_secret" => "",
];