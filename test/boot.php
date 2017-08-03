<?php

use Zan\Framework\Components\Nsq\NsqConfig;
use Zan\Framework\Components\Nsq\Utils\Binary;
use Zan\Framework\Components\Nsq\Utils\MemoryBuffer;
use Zan\Framework\Components\Nsq\Utils\ObjectPool;
use Zan\Framework\Foundation\Core\Debug;
use ZanPHP\SPI\AliasLoader;
use ZanPHP\SPI\ServiceLoader;

require_once __DIR__ . "/../vendor/autoload.php";

$vendor = __DIR__ . '/../vendor/';
$aliasLoader = AliasLoader::getInstance();
$aliasLoader->scan($vendor);

$serviceLoader = ServiceLoader::getInstance();
$serviceLoader->scan($vendor);

Debug::detect();

$config = require_once __DIR__ . "/config.php";
NsqConfig::init($config);

ObjectPool::create(new Binary(new MemoryBuffer(8192)), 30);
