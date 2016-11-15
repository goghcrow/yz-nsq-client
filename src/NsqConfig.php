<?php

namespace Zan\Framework\Components\Nsq;


use Zan\Framework\Utilities\Types\Arr;

class NsqConfig
{
    const CONF_PATH = "nsq";
    const UA = "zan-nsq-client";
    const VER = "v0.1";

    private static $identify;
    private static $nsqlookupdConnectTimeout;
    private static $nsqdConnectTimeout;
    private static $maxInFlightCount;
    private static $packetSizeLimit;
    private static $socketBufferSize;
    private static $delayingCloseTime;
    private static $messageBackoff;
    private static $maxBackoffDuration;
    private static $lookupdPollInterval;
    private static $lookupdPollJitter;
    private static $rdyRedistributeInterval;
    private static $lowRdyIdleTimeout;
    private static $maxAttempts;
    private static $enableBackoff;
    private static $maxConnectionPerTopic;
    private static $lookup;
    private static $topic;
    private static $publishTimeout;
    private static $disposableConnLifecycle;
    private static $messageAutoResponse;

    // TODO 压平client参数调整参数
    // 干掉静态变量
    public static function init(array $config)
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

        static::$identify                   = Arr::get($config, "identity", [])                + $idInfoDefault;
        static::$messageBackoff             = Arr::get($config, "message_backoff", [])         + $backoffDefault;
        static::$messageAutoResponse        = Arr::get($config, "message_auto_response",       true);
        static::$maxBackoffDuration         = Arr::get($config, "max_backoff_duration",        60 * 1000);
        static::$nsqlookupdConnectTimeout   = Arr::get($config, "nsqlookupd_connect_timeout",  3 * 1000);
        static::$nsqdConnectTimeout         = Arr::get($config, "nsqd_connect_timeout",        3 * 1000);
        static::$socketBufferSize           = Arr::get($config, "socket_buffer_size",          327679);
        static::$packetSizeLimit            = Arr::get($config, "packet_size_limit",           327679);
        static::$lookupdPollInterval        = Arr::get($config, "lookupd_poll_interval",       60 * 1000);
        static::$lookupdPollJitter          = Arr::get($config, "lookupd_poll_jitter",         0.3);
        static::$maxInFlightCount           = Arr::get($config, "max_in_flight",               2500);
        static::$delayingCloseTime          = Arr::get($config, "delaying_close_time",         5 * 1000);
        static::$rdyRedistributeInterval    = Arr::get($config, "rdy_redistribute_interval",   5 * 1000);
        static::$lowRdyIdleTimeout          = Arr::get($config, "low_rdy_idle_timeout",        10 * 1000);
        static::$maxAttempts                = Arr::get($config, "max_attempts",                5);
        static::$enableBackoff              = Arr::get($config, "enable_backoff",              false);
        static::$lookup                     = Arr::get($config, "lookup",                      []);
        static::$topic                      = Arr::get($config, "topic",                       []);
        static::$maxConnectionPerTopic      = Arr::get($config, "max_connection_per_topic",    1);
        static::$publishTimeout             = Arr::get($config, "publish_timeout",             3 * 1000);
        static::$disposableConnLifecycle    = Arr::get($config, "disposable_connection_lifecycle", 3 * 1000);
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

    public static function getMaxConnectionPerTopic()
    {
        return static::$maxConnectionPerTopic;
    }

    public static function getLookup()
    {
        return static::$lookup;
    }

    public static function getTopic()
    {
        return static::$topic;
    }

    public static function getPublishTimeout()
    {
        return static::$publishTimeout;
    }

    public static function getDisposableConnLifecycle()
    {
        return static::$disposableConnLifecycle;
    }

    public static function getMessageAutoResponse()
    {
        return static::$messageAutoResponse;
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