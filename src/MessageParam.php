<?php
namespace Zan\Framework\Components\Nsq;

class MessageParam {
    private $params = [];
    
    function withTag($tag)
    {
        $this->params['##client_dispatch_tag'] = strval($tag);
        return $this;
    }
    
    function withTraceId($traceId)
    {
        $this->params['##trace_id'] = (string)$traceId;
        return $this;
    }
    
    function withCustom($name, $value)
    {
        if (substr($name, 0, 2) == '##') {
            throw new NsqException('custom extends can not starts with "##"');
        }
        $this->params[$name] = (string)$value;
        return $this;
    }
    
    function toArray()
    {
        return $this->params;
    }
    
    function getTag()
    {
        return isset($this->params['##client_dispatch_tag']) ? $this->params['##client_dispatch_tag'] : null;
    }
    
    function getTraceId()
    {
        return isset($this->params['##trace_id']) ? $this->params['##trace_id'] : null;
    }
    
    static function fromArray($array)
    {
        $ret = new MessageParam();
        $ret->params = $array;
        return $ret;
    }
}

