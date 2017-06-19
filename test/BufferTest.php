<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Utils\MemoryBuffer;
use Zan\Framework\Components\Nsq\Utils\StringBuffer;

require_once __DIR__ . "/boot.php";

$buffer = new StringBuffer();
$buffer->write("1234");
assert($buffer->read(1) === "1");
assert($buffer->__toString() === "234");
$buffer->write("56");
assert($buffer->__toString() === "23456");
assert($buffer->read(2) === "23");
$buffer->write("789");
assert($buffer->__toString() === "456789");


$buffer = new MemoryBuffer(5);
$buffer->write("1234");
assert($buffer->read(1) === "1");
assert($buffer->__toString() === "234");
$buffer->write("56");
assert($buffer->__toString() === "23456");
assert($buffer->writableBytes() === 0);
assert($buffer->capacity() === 5);
assert($buffer->read(2) === "23");
assert($buffer->prependableBytes() === 2);
assert($buffer->writableBytes() === 0);
$buffer->write("789");
assert($buffer->prependableBytes() === 0);
assert($buffer->readableBytes() === 6);
assert($buffer->writableBytes() === 6);
assert($buffer->capacity() === 12);