<?php

namespace Zan\Framework\Components\Nsq\Utils;

use swoole_buffer as SwooleBuffer;
use Zan\Framework\Components\Nsq\Contract\Buffer;


/**
 * Class Buffer
 *
 * 自动扩容, 从尾部写入数据，从头部读出数据
 * 参考 
 *
 * +-------------------+------------------+------------------+
 * | prependable bytes |  readable bytes  |  writable bytes  |
 * |                   |     (CONTENT)    |                  |
 * +-------------------+------------------+------------------+
 * |                   |                  |                  |
 * V                   V                  V                  V
 * 0      <=      readerIndex   <=   writerIndex    <=     size
 */
class MemoryBuffer implements Buffer
{
    private $buffer;

    private $readerIndex;

    private $writerIndex;

    public static function ofBytes($bytes)
    {
        $self = new static;
        $self->write($bytes);
        return $self;
    }

    public function __construct($size = 1024)
    {
        $this->buffer = new SwooleBuffer($size);
        $this->readerIndex = 0;
        $this->writerIndex = 0;
    }

    public function __clone()
    {
        $this->reset();
    }

    public function readableBytes()
    {
        return $this->writerIndex - $this->readerIndex;
    }

    public function writableBytes()
    {
        return $this->buffer->capacity - $this->writerIndex;
    }

    public function prependableBytes()
    {
        return $this->readerIndex;
    }

    public function capacity()
    {
        return $this->buffer->capacity;
    }

    public function get($len)
    {
        if ($len <= 0) {
            return "";
        }

        $len = min($len, $this->readableBytes());
        return $this->rawRead($this->readerIndex, $len);
    }

    public function read($len)
    {
        if ($len <= 0) {
            return "";
        }

        $len = min($len, $this->readableBytes());
        $read = $this->rawRead($this->readerIndex, $len);
        $this->readerIndex += $len;
        if ($this->readerIndex === $this->writerIndex) {
            $this->reset();
        }
        return $read;
    }

    public function readFull()
    {
        return $this->read($this->readableBytes());
    }

    public function write($bytes)
    {
        if ($bytes === "") {
            return false;
        }

        $len = strlen($bytes);

        if ($len <= $this->writableBytes()) {

            write:
            $this->rawWrite($this->writerIndex, $bytes);
            $this->writerIndex += $len;
            return true;
        }

        // expand
        if ($len > ($this->prependableBytes() + $this->writableBytes())) {
            $this->expand(($this->readableBytes() + $len) * 2);
        }

        // copy-move
        if ($this->readerIndex !== 0) {
            $this->rawWrite(0, $this->rawRead($this->readerIndex, $this->writerIndex - $this->readerIndex));
            $this->writerIndex -= $this->readerIndex;
            $this->readerIndex = 0;
        }

        goto write;
    }

    public function reset()
    {
        $this->readerIndex = 0;
        $this->writerIndex = 0;
    }

    public function __toString()
    {
        return $this->rawRead($this->readerIndex, $this->writerIndex - $this->readerIndex);
    }

    // NOTICE: 影响 IDE Debugger
    public function __debugInfo()
    {
        return [
            "string" => $this->__toString(),
            "capacity" => $this->capacity(),
            "readerIndex" => $this->readerIndex,
            "writerIndex" => $this->writerIndex,
            "prependableBytes" => $this->prependableBytes(),
            "readableBytes" => $this->readableBytes(),
            "writableBytes" => $this->writableBytes(),
        ];
    }

    private function rawRead($offset, $len)
    {
        if ($offset < 0 || $offset + $len > $this->buffer->capacity) {
            throw new \InvalidArgumentException(__METHOD__ . ": offset=$offset, len=$len, capacity={$this->buffer->capacity}");
        }
        return $this->buffer->read($offset, $len);
    }

    private function rawWrite($offset, $bytes)
    {
        $len = strlen($bytes);
        if ($offset < 0 || $offset + $len > $this->buffer->capacity) {
            throw new \InvalidArgumentException(__METHOD__ . ": offset=$offset, len=$len, capacity={$this->buffer->capacity}");
        }
        return $this->buffer->write($offset, $bytes);
    }

    private function expand($size)
    {
        if ($size <= $this->buffer->capacity) {
            throw new \InvalidArgumentException(__METHOD__ . ": size=$size, capacity={$this->buffer->capacity}");
        }
        return $this->buffer->expand($size);
    }
}

/*
 * swoole_buffer的C实现不是很靠谱
 *
 * 1. buffer不是string, substr方法参数有问题, 容易用错
 * 2. append方法BUG, 类属性length应该减去buffer->offset
 *      bug: // zend_update_property_long(swoole_buffer_class_entry_ptr, getThis(), ZEND_STRL("length"), buffer->length TSRMLS_CC);
 *      fix: zend_update_property_long(swoole_buffer_class_entry_ptr, getThis(), ZEND_STRL("length"), buffer->length - buffer->offset TSRMLS_CC);
 * 3. read与write方法受到buffer->offset约束, offset之前数据对其不可见
 * 4. write方法写入数据, 未处理buffer->length, 导致对substr于__toString方法不可见, 只能用read取出
 *
 * 方案:
 *
 * 1. 不使用除expand, read与write外其他方法, 不改变内部swString的offset与length字段, 使其保持初值0
 * 2. 有条件的使用write与read, 保证offset参数必须大于0, 原因见代码
 *      write(offset, str)
 *      read(offset, len)
 *      expand(size)

static PHP_METHOD(swoole_buffer, write)
{
    long offset;
    char *new_str;
    zend_size_t length;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls", &offset, &new_str, &length) == FAILURE)
    {
        RETURN_FALSE;
    }
    swString *buffer = swoole_get_object(getThis());
    if (offset < 0)
    {
        // 没有append任何数据, 此处buffer->length == 0
        offset = buffer->length + offset;
    }
    // 不调用substr(,,true)移动offset下标, 此处buffer->offset == 0
    offset += buffer->offset;
    if (length > buffer->size - offset)
    {
        php_error_docref(NULL TSRMLS_CC, E_WARNING, "string is too long.");
        RETURN_FALSE;
    }
    memcpy(buffer->str + offset, new_str, length);
    RETURN_TRUE;
}

static PHP_METHOD(swoole_buffer, read)
{
    long offset;
    long length;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ll", &offset, &length) == FAILURE)
    {
        RETURN_FALSE;
    }
    swString *buffer = swoole_get_object(getThis());
    if (offset < 0)
    {
        offset = buffer->length + offset;
    }
    offset += buffer->offset;
    if (length > buffer->size - offset)
    {
        php_error_docref(NULL TSRMLS_CC, E_WARNING, "no enough data.");
        RETURN_FALSE;
    }
    SW_RETURN_STRINGL(buffer->str + offset, length, 1);
}
*/