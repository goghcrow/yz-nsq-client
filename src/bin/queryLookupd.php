<?php

use Zan\Framework\Components\Nsq\Dns;
use Zan\Framework\Components\Nsq\NsqConfig;
use Zan\Framework\Foundation\Coroutine\Task;

require_once __DIR__ . "/../../vendor/autoload.php";
NsqConfig::init();
\Zan\Framework\Foundation\Core\Debug::detect();

//$json = file_get_contents("http://nsq-dev.s.qima-inc.com:4161/lookup?topic=zan_mqworker_test");
//var_export(json_decode($json, true));
//array (
//    'status_code' => 200,
//    'status_txt' => 'OK',
//    'data' =>
//        array (
//            'channels' =>
//                array (
//                    0 => 'ch1',
//                    1 => 'ch2',
//                ),
//            'producers' =>
//                array (
//                    0 =>
//                        array (
//                            'remote_address' => '127.0.0.1:33004',
//                            'hostname' => 'qabb-dev-nsq0',
//                            'broadcast_address' => '10.9.6.49',
//                            'tcp_port' => 4150,
//                            'http_port' => 4151,
//                            'version' => '0.3.7',
//                        ),
//                ),
//        ),
//);
//exit;


function lookupd()
{
    $endpoint = "http://nsq-dev.s.qima-inc.com:4161/lookup?topic=zan_mqworker_test";
    $host = parse_url($endpoint, PHP_URL_HOST);
    $port = parse_url($endpoint, PHP_URL_PORT) ?: 80;
    $path = parse_url($endpoint, PHP_URL_PATH);
    $query = parse_url($endpoint, PHP_URL_QUERY);
    $uri = "$path?$query";
    $ip = (yield Dns::lookup($host));
    $httpClient = new \Zan\Framework\Network\Common\HttpClient($ip, $port);

    /* @var \Zan\Framework\Network\Common\Response $resp */
    $resp = (yield $httpClient->get($uri));
    if ($resp->getStatusCode() !== 200) {
        // f
    }
    $respArr = json_decode($resp->getBody(), true, 512, JSON_BIGINT_AS_STRING);
    if (!isset($respArr["status_code"]) ||
        $respArr["status_code"] !== 200 ||
        !isset($respArr["data"]) ||
        !$respArr["data"]) {
        // f
    }

    $data = $respArr["data"];
    $nsqdAddrs = [];

    foreach ($data["producers"] as $producer)
    {
        $host = $producer["broadcast_address"];
        $port = $producer["tcp_port"];
        $nsqdAddrs[] = "$host:$port";
    }

    var_dump($nsqdAddrs);
}
Task::execute(lookupd());