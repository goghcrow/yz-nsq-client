<?php

namespace Zan\Framework\Components\Nsq\Utils;


class Binary extends Buffer
{
    public function writeUInt8($i)
    {
        return $this->write(pack('C', $i));
    }

    public function writeUInt16BE($i)
    {
        return $this->write(pack('n', $i));
    }

    public function writeUInt16LE($i)
    {
        return $this->write(pack('v', $i));
    }

    public function writeUInt32BE($i)
    {
        return $this->write(pack('N', $i));
    }

    public function writeUInt32LE($i)
    {
        return $this->write(pack('V', $i));
    }

    public function writeUInt64BE($uint64Str)
    {
        $low = bcmod($uint64Str, "4294967296");
        $hi = bcdiv($uint64Str, "4294967296", 0);
        return $this->write(pack("NN", $hi, $low));
    }

    public function writeUInt64LE($uint64Str)
    {
        $low = bcmod($uint64Str, "4294967296");
        $hi = bcdiv($uint64Str, "4294967296", 0);
        return $this->write(pack('VV', $low, $hi));
    }

    public function writeInt32BE($i)
    {
        return $this->write(pack('N', $i));
    }

    public function writeInt32LE($i)
    {
        return $this->write(pack('V', $i));
    }

    public function writeFloat($f)
    {
        return $this->write(pack('f', $f));
    }

    public function writeDouble($d)
    {
        return $this->write(pack('d', $d));
    }

    public function readUInt8()
    {
        $ret = unpack("Cr", $this->read(1));
        return $ret == false ? null : $ret["r"];
    }

    public function readUInt16BE()
    {
        $ret = unpack("nr", $this->read(2));
        return $ret === false ? null : $ret["r"];
    }

    public function readUInt16LE()
    {
        $ret = unpack("vr", $this->read(2));
        return $ret === false ? null : $ret["r"];
    }

    public function readUInt32BE()
    {
        $ret = unpack("nhi/nlo", $this->read(4));
        return $ret === false ? null : (($ret["hi"] << 16) | $ret["lo"]);
    }

    public function readUInt32LE()
    {
        $ret = unpack("vlo/vhi", $this->read(4));
        return $ret === false ? null : (($ret["hi"] << 16) | $ret["lo"]);
    }

    public function readUInt64BE()
    {
        $param = unpack("Nhi/Nlow", $this->read(8));
        return bcadd(bcmul($param["hi"], "4294967296", 0), $param["low"]);
    }

    public function readUInt64LE()
    {
        $param = unpack("Vlow/Vhi", $this->read(8));
        return bcadd(bcmul($param["hi"], "4294967296", 0), $param["low"]);
    }

    public function readInt32BE()
    {
        $ret = unpack("Nr", $this->read(4));
        return $ret === false ? null : $ret["r"];
    }

    public function readInt32LE()
    {
        $ret = unpack("Vr", $this->read(4));
        return $ret === false ? null : $ret["r"];
    }

    public function readFloat()
    {
        $ret = unpack("fr", $this->read(4));
        return $ret === false ? null : $ret["r"];
    }

    public function readDouble()
    {
        $ret = unpack("dr", $this->read(8));
        return $ret === false ? null : $ret["r"];
    }
}