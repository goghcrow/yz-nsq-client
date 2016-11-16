<?php

namespace Zan\Framework\Components\Nsq;

use Zan\Framework\Components\Nsq\Utils\Binary;


class Frame
{
    /**
     * [space][space][V][2]
     */
    const MAGIC_V2 = "  V2";

    const HEARTBEAT = '_heartbeat_';

    const FrameTypeResponse = 0;
    const FrameTypeError    = 1;
    const FrameTypeMessage  = 2;

    private $type;

    private $body;

    public function __construct($bytes)
    {
        $this->unpack($bytes);
    }

    public function __destruct()
    {
        foreach (get_class_vars(__CLASS__) as $prop => $_) {
            unset($this->$prop);
        }
    }

    /**
     * @return int
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * @return string
     */
    public function getBody()
    {
        return $this->body;
    }

    /**
     * @param string $bytes
     *
     * NSQ protocol
     * [x][x][x][x][x][x][x][x][x][x][x][x]...
     * |  (int32) ||  (int32) || (binary)
     * |  4-byte  ||  4-byte  || N-byte
     * ------------------------------------...
     *    size     frame type     data
     *
     *  size  =    strlen(frame type) + strlen(data)
     *  sizeof(frameType:int) == 4
     * @throws NsqException
     */
    private function unpack($bytes)
    {
        if (strlen($bytes) < 4) {
            throw new NsqException("length of response is too small");
        }

        $binary = new Binary();
        $binary->write($bytes);
        $bodySize = $binary->readInt32BE() - 4;
        $this->type = $binary->readInt32BE();
        $this->body = $binary->read($bodySize);
    }
}