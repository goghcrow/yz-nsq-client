<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Foundation\Core\Config;

class NsqConfig
{
    const KEY = "nsq";
    const UA = "zan-nsq-client";
    const VER = "v0.1";

    private static $identify;
    private static $nsqlookupdConnectTimeout;
    private static $nsqdConnectTimeout;
    private static $maxInFlightCount;
    private static $packetSizeLimit;
    private static $socketBufferSize;
    private static $delayingCloseTime;
    private static $maxRequeueDelay;
    private static $messageBackoff;
    private static $maxBackoffDuration;
    private static $lookupdPollInterval;
    private static $lookupdPollJitter;
    private static $rdyRedistributeInterval;
    private static $lowRdyIdleTimeout;
    private static $maxAttempts;
    private static $enableBackoff;
    private static $maxConnectionPerNsqd;

    // TODO 压平client参数调整参数
    // 干掉静态变量
    public static function init()
    {
        $hostname = gethostname();

        $idInfoDefault = [
            "client_id" => $hostname,
            "hostname" => $hostname,
            "feature_negotiation" => true, // TODO
            "heartbeat_interval" => 30 * 1000, // default: 30000ms
            "output_buffer_size" => 16384, // 16kb
            "output_buffer_timeout" => -1, // default 250ms
            "tls_v1" => false,
            "snappy" => false,
            "deflate" => false,
            "deflate_level" => 1,
            "sample_rate" => 0,
            "user_agent" => static::UA . "/" . static::VER,
            "msg_timeout" => 60000, // ms
        ];

        $backoffDefault = [
            "min" => 2000, // 2s
            "max" => 1000 * 60 * 10, // min
            "factor" => 2,
            "jitter" => 0.3 // 0~1
        ];

        static::$identify                   = Config::get(static::KEY . ".identity", [])                + $idInfoDefault;
        static::$messageBackoff             = Config::get(static::KEY . ".message_backoff", [])         + $backoffDefault;
        static::$maxBackoffDuration         = Config::get(static::KEY . ".max_backoff_duration",        60 * 1000);
        static::$nsqlookupdConnectTimeout   = Config::get(static::KEY . ".nsqlookupd_connect_timeout",  3 * 1000);
        static::$nsqdConnectTimeout         = Config::get(static::KEY . ".nsqd_connect_timeout",        3 * 1000);
        static::$socketBufferSize           = Config::get(static::KEY . ".socket_buffer_size",          327679);
        static::$packetSizeLimit            = Config::get(static::KEY . ".packet_size_limit",           327679);
        static::$lookupdPollInterval        = Config::get(static::KEY . ".lookupd_poll_interval",       60 * 1000);
        static::$lookupdPollJitter          = Config::get(static::KEY . ".lookupd_poll_jitter",         0.3);
        static::$maxInFlightCount           = Config::get(static::KEY . ".max_in_flight",               2500);
        static::$maxRequeueDelay            = Config::get(static::KEY . ".max_requeue_delay",           60 * 1000 * 1000);
        static::$delayingCloseTime          = Config::get(static::KEY . ".delaying_close_time",         5 * 1000);
        static::$rdyRedistributeInterval    = Config::get(static::KEY . ".rdy_redistribute_interval",   5 * 1000);
        static::$lowRdyIdleTimeout          = Config::get(static::KEY . ".low_rdy_idle_timeout",        10 * 1000);
        static::$maxAttempts                = Config::get(static::KEY . ".max_attempts",                5);
        static::$enableBackoff              = Config::get(static::KEY . ".enable_backoff",              true); // TODO
        static::$maxConnectionPerNsqd       = Config::get(static::KEY . ".max_connection_per_nsqd",     1); // TODO
    }

    public static function getDelayingCloseTime()
    {
        return static::$delayingCloseTime;
    }

    public static function getNsqdConnectTimeout()
    {
        return static::$nsqdConnectTimeout;
    }

    public static function getNsqlookupdConnectTimeout()
    {
        return static::$nsqlookupdConnectTimeout;
    }

    public static function getIdentity()
    {
        return static::$identify;
    }

    public static function negotiateIdentity($identity)
    {
        static::$identify = array_merge(static::$identify, $identity);
    }

    public static function getMaxInFlightCount()
    {
        return static::$maxInFlightCount;
    }

    public static function getPacketSizeLimit()
    {
        return static::$packetSizeLimit;
    }

    public static function getSocketBufferSize()
    {
        return static::$socketBufferSize;
    }

    public static function getMaxRequeueDelay()
    {
        return static::$maxRequeueDelay;
    }

    public static function getMessageBackoff()
    {
        return static::$messageBackoff;
    }

    public static function getMaxBackoffDuration()
    {
        return static::$maxBackoffDuration;
    }

    public static function getLookupdPollInterval()
    {
        return static::$lookupdPollInterval;
    }

    public static function getLookupdPollJitter()
    {
        return static::$lookupdPollJitter;
    }

    public static function getRdyRedistributeInterval()
    {
        return static::$rdyRedistributeInterval;
    }

    public static function getLowRdyIdleTimeout()
    {
        return static::$lowRdyIdleTimeout;
    }

    public static function getMaxAttempts()
    {
        return static::$maxAttempts;
    }

    public static function enableBackoff()
    {
        return static::$enableBackoff;
    }

    public static function getMaxConnectionPerNsqd()
    {
        return static::$maxConnectionPerNsqd;
    }

    public static function getMaxRDYCount()
    {
        if (isset(static::$identify["max_rdy_count"])) {
            return static::$identify["max_rdy_count"];
        } else {
            return 2500;
        }
    }
}


// TODO 静态变量转变为一份配置文件表
[
    "max_connection_per_nsqd" => 10,
    "enable_backoff" => true,

    // prod机器 /proc/sys/net/core/rmem_max = /proc/sys/net/core/wmem_max = 327679
    "socket_buffer_size" => 327679,
    "packet_size_limit" => 327679,
    "delaying_close_time" => 5000, // ms
    "nsqd_connect_timeout" => 1000, // ms
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
    "lookupd_poll_interval" => 60000, //60s
    "lookupd_poll_jitter" => 0.3,

    // Maximum number of messages to allow in flight (concurrency knob)
    "max_in_flight" => 2500, // pipe_count

    // Maximum duration when REQueueing (for doubling of deferred requeue)
    "max_requeue_delay" => 60, //s TODO
    "default_requeue_delay" => 90, //s TODO

    "max_backoff_duration" => 10 * 60 * 1000, // 10min

    "message_backoff" => [
        "min" => 2000, // s // TODO 选择一个合适的min , 跟attemp有关
        // Maximum amount of time to backoff when processing fails 0 == no backoff
        "max" => 1000 * 60 * 10, // 10 min
        "factor" => 2,
        "jitter" => 0.3 // 0~1
    ],

    // secret for nsqd authentication (requires nsqd 0.2.29+)
    // "auto_secret" => "",

    // Duration to wait for a message from a producer when in a state where RDY
    // counts are re-distributed (ie. max_in_flight < num_producers)
    "low_rdy_idle_timeout" => 10 * 1000, // 10s

    // Duration between redistributing max-in-flight to connections
    "rdy_redistribute_interval" => 5 * 1000, //5s

    // Maximum number of times this consumer will attempt to process a message before giving up
    "max_attempts" => 5,
];