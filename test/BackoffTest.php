<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Utils\Backoff;

require_once __DIR__ . "/boot.php";

$min = 2000;
$max = 1000 * 60 * 10;
$factor = 2;
$jitter = 0.3;
$maxAttempt = 5;

$backoff = new Backoff($min, $max, $factor, $jitter);
for ($i = 0; $i < $maxAttempt; $i++) {
    echo $backoff->duration($i + 1), "\n";
}
