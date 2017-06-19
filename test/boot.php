<?php

use Zan\Framework\Components\Nsq\NsqConfig;
use Zan\Framework\Components\Nsq\Utils\Binary;
use Zan\Framework\Components\Nsq\Utils\MemoryBuffer;
use Zan\Framework\Components\Nsq\Utils\ObjectPool;
use Zan\Framework\Foundation\Core\Debug;

require_once __DIR__ . "/../vendor/autoload.php";
Debug::detect();

$config = require_once __DIR__ . "/config.php";
NsqConfig::init($config);

ObjectPool::create(new Binary(new MemoryBuffer(8192)), 30);