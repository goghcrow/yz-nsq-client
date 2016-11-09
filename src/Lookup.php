<?php

namespace Zan\Framework\Components\Nsq;

use Zan\Framework\Network\Common\HttpClient;
use Zan\Framework\Network\Common\Response;


class Lookup
{
    private $addr;

    public function __construct($addr)
    {
        $this->addr = $addr;
    }

    public function queryNsqd($topic)
    {
        Command::checkTopicChannelName($topic);

        /* @var \Zan\Framework\Network\Common\Response $resp */
        $host = parse_url($this->addr, PHP_URL_HOST);
        $port = parse_url($this->addr, PHP_URL_PORT) ?: 80;

        $host = (yield Dns::lookup($host, 1000));// ?: $host;
        $httpClient = new HttpClient($host, $port);
        $resp = (yield $httpClient->get("/lookup", ["topic" => $topic], NsqConfig::getNsqlookupdConnectTimeout()));
        $data = static::validLookupdResp($resp);

        $nsqdList = [];
        foreach ($data["producers"] as $producer)
        {
            $nsqdList[] = [$producer["broadcast_address"], $producer["tcp_port"]];
        }

        yield $nsqdList;
    }

    private static function validLookupdResp(Response $resp)
    {
        $statusCode = $resp->getStatusCode();
        $body = $resp->getBody();
        if ($statusCode !== 200) {
            throw new NsqException("queryLookupd failed [http_status_code=$statusCode]");
        }

        $respArr = json_decode($resp->getBody(), true, 512, JSON_BIGINT_AS_STRING);
        if (!isset($respArr["status_code"]) ||
            $respArr["status_code"] !== 200 ||
            !isset($respArr["data"]) ||
            !$respArr["data"]) {
            throw new NsqException("queryLookupd failed, invalid resp [body=$body]");
        }
        return $respArr["data"];
    }
}