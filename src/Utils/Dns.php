<?php

namespace Zan\Framework\Components\Nsq\Utils;

use Zan\Framework\Foundation\Contract\Async;
use Zan\Framework\Network\Connection\Exception\ConnectTimeoutException;
use Zan\Framework\Network\Server\Timer\Timer;

class Dns implements Async
{
    private $callback;

    public static function lookup($domain, $timeout = 1000)
    {
        $self = new static;

        $timeoutId = Timer::after($timeout, function() use($self, $domain, $timeout) {
            if ($self->callback) {
                call_user_func($self->callback, $domain, new ConnectTimeoutException("Dns lookup timeout {$timeout}ms"));
                unset($self->callback);
            }
        });

        // 无需做缓存, 内部有缓存
        swoole_async_dns_lookup($domain, function($domain, $ip) use($self, $timeoutId) {
            if ($self->callback) {
                Timer::clearAfterJob($timeoutId);
                call_user_func($self->callback, $ip ?: $domain);
                unset($self->callback);
            }
        });

        return $self;
    }

    public function execute(callable $callback, $task)
    {
        $this->callback = $callback;
    }
}