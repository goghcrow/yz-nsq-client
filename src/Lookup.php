<?php

namespace Zan\Framework\Components\Nsq;

use Zan\Framework\Components\Nsq\Contract\NsqdDelegate;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Common\HttpClient;
use Zan\Framework\Network\Common\Response;
use Zan\Framework\Network\Server\Timer\Timer;


class Lookup
{
    private $topic;

    private $rw;
    
    const R = 'r';
    const W = 'w';
    
    private $isStopped = false;

    private $extendSupport = false;

    private $extraIdentifyParams = [];
    
    /**
     * [ object_hash=>Connection ]
     * @var Connection[] map<string, Connection>
     */
    private $pendingConnections = [];

    /**
     * [ object_hash=>Connection ]
     * @var Connection[] map<string, Connection>
     */
    private $idleConnections = [];
    
    /**
     * [ object_hash=>Connection ]
     * @var Connection[]
     */
    private $busyConnections = [];

    /**
     *
     * e.g. [ 0 => [object_hash=>Connection,... ], ]
     *    
     */
    private $partitionIdleConnections = [];
    
    private $nodes;
    
    private $nodePartitions;
    
    private $partitionNode;
    
    /**
     * connecting or connected
     * @var string[] map<string, int> [ "host:port" => conn_num, ]
     */
    private $nsqdTCPAddrsConnNum = [];

    /**
     * max connection in current lookup
     * @var int
     */
    private $maxConnectionNum = 0;

    /**
     * @var NsqdDelegate
     */
    private $delegate;

    /**
     * @var string[]  map<string, list> [ "http://nsq-dev.s.qima-inc.com:4161" => [], ... ]
     */
    private $lookupdHTTPAddrs = [];
    private $lookupdQueryIndex = 0; // for round-robin
    private $lookupRetries = 3;

    public function __construct($topic, $rw, $maxConnectionNum = 1)
    {
        $this->topic = Command::checkTopicChannelName($topic);
        $this->maxConnectionNum = $maxConnectionNum;
        $this->rw = $rw;
    }

    public function setNsqdDelegate(NsqdDelegate $delegate)
    {
        $this->delegate = $delegate;
    }

    public function setExtraIdentifyParams($params)
    {
        $this->extraIdentifyParams = $params;
    }

    /**
     * ConnectToNSQLookupd adds an nsqlookupd address to the list for this Consumer instance.
     * If it is the first to be added, it initiates an Tick Timer to discover nsqd
     * producers for the configured topic.
     *
     * @param string $addr
     * @return \Generator|void Connection[]
     * @throws NsqException
     */
    public function connectToNSQLookupd($addr)
    {
        if ($this->isStopped) {
            throw new NsqException("consumer stopped");
        }

        $addr = $this->formatAddress($addr);
        if (isset($this->lookupdHTTPAddrs[$addr])) {
            return;
        }

        $this->lookupdHTTPAddrs[$addr] = [];
        yield $this->queryLookupd($addr);

        if (count($this->lookupdHTTPAddrs) === 1) {
            // reconnecting will be triggered on connection close,
            // so no need to check number of connections in tick timer
            $this->startLookupdPollingTick();
        }
    }

    /**
     * an Tick Timer to discover nsqd producers for the configured topic.
     * @return void
     */
    private function startLookupdPollingTick()
    {
        // add some jitter
        $jitter = (float)rand() / (float)getrandmax()
            * NsqConfig::getLookupdPollJitter()
            * NsqConfig::getLookupdPollInterval();

        Timer::after(intval($jitter) + 1, function() {
            if ($this->isStopped) {
                return;
            }

            Timer::tick(NsqConfig::getLookupdPollInterval(), function() {
                try {
                    Task::execute($this->queryLookupd());
                } catch (\Throwable $t) {
                    sys_echo("lookupdPolling exception {$t->getMessage()}");
                } catch (\Exception $ex) {
                    sys_echo("lookupdPolling exception {$ex->getMessage()}");
                }
            }, $this->lookupdPollingTickId());
        });
    }

    /**
     * DisconnectFromNSQLookupd removes the specified `nsqlookupd` address
     * from the list used for periodic discovery and close all nsqd connection
     * discover from the address
     *
     * @param string $addr
     * @return void
     * @throws NsqException
     */
    public function disconnectFromNSQLookupd($addr)
    {
        $addr = $this->formatAddress($addr);
        if (!isset($this->lookupdHTTPAddrs[$addr])) {
            throw new NsqException("not connected");
        }

        if (count($this->lookupdHTTPAddrs) === 1) {
            throw new NsqException("cannot disconnect from only remaining nsqlookupd HTTP address $addr");
        }

        unset($this->lookupdHTTPAddrs[$addr]);

        foreach ($this->getConnections() as $conn) {
            if ($conn->getLookupAddr() === $addr) {
                $conn->tryClose();
            }
        }
    }

    /**
     * make an HTTP req to one of the configured nsqlookupd instances (Round-Robin)
     * to discover which nsqd's provide the topic we are consuming.
     * initiate a connection to any new producers that are identified.
     * @param null|string $lookupdAddr
     * @return \Generator
     */
    public function queryLookupd($lookupdAddr = null)
    {
        $lookupResult = (yield $this->lookupWithRetry($lookupdAddr, $this->lookupRetries));
        if (isset($lookupResult['meta']['extend_support']) && $lookupResult['meta']['extend_support']) {
            $this->extendSupport = true;
        }
        $nsqdList = $this->getNodeList($lookupResult);
        $this->lookupdHTTPAddrs[$lookupdAddr] = $nsqdList;
        yield $this->connectToNSQDList($nsqdList);
    }
    
     /**
     * initiate connections to any new producers that are identified.
     * @param array $nsqdList
     * @return \Generator
     */
    public function connectToNSQDList($nsqdList) {
        foreach ($nsqdList as list($host, $port)) {
            if (!isset($this->nsqdTCPAddrsConnNum["$host:$port"])) {
                $this->nsqdTCPAddrsConnNum["$host:$port"] = 0;
            }
        }
        $this->maxConnectionNum = max(count($nsqdList), $this->maxConnectionNum);
        foreach ($nsqdList as list($host, $port)) {
            try {
                /* @var Connection[] $conns */
                $conns = (yield $this->connectToNSQD($host, $port));
                foreach ($conns as $conn) {
                    //$conn->setLookupAddr($lookupdAddr);
                    if ($this->delegate) {
                        $this->delegate->onConnect($conn);
                    }
                }
            } catch (\Throwable $t) {
                sys_echo("($host:$port) error connecting to nsqd - {$t->getMessage()}");
            } catch (\Exception $ex) {
                sys_echo("($host:$port) error connecting to nsqd - {$ex->getMessage()}");
            }
        }
    }

    private function lookupWithRetry($lookupdAddr, $n = 3)
    {
        $nsqdList = [];
        $lookupdAddr = $lookupdAddr ?: $this->nextLookupdEndpoint();
        if ($lookupdAddr === null) {
            return;
        }

        try {
            $lookupResult = (yield $this->lookup($lookupdAddr, $this->topic));
        } catch (\Throwable $ex) {
        } catch (\Exception $ex) { }

        if (isset($ex)) {
            sys_echo("($lookupdAddr) error query nsqd - {$ex->getMessage()}");
            echo_exception($ex);

            if (--$n > 0) {
                yield taskSleep(500);
                yield $this->lookupWithRetry($lookupdAddr, $n);
            } else {
                yield $lookupResult;
            }
        } else {
            yield $lookupResult;
        }
    }

    private function getNodeList($lookupResult) {
        $nsqdList = [];
        $nodes = [];
        $partitionNode = null;
        $nodePartitions = null;
        
        foreach ($nsqdList as list($host, $port)) {
            if (!isset($this->nsqdTCPAddrsConnNum["$host:$port"])) {
                $this->nsqdTCPAddrsConnNum["$host:$port"] = 0;
            }
        }
        foreach ($lookupResult["producers"] as $producer) {
            $ip = $producer["broadcast_address"];
            $port = $producer["tcp_port"];
            $key = "$ip:$port";
            $nodes[$key] = [$ip, $port];
            if (!isset($this->nsqdTCPAddrsConnNum[$key])) {
                $this->nsqdTCPAddrsConnNum[$key] = 0;
            }
        }
        if (isset($lookupResult["partitions"])) {
            $partitionNode = [];
            $nodePartitions = [];
            foreach ($lookupResult["partitions"] as $partition=>$producer) {
                $ip = $producer["broadcast_address"];
                $port = $producer["tcp_port"];
                $key = "$ip:$port";
                $nodes[$key] = [$ip, $port];
                if (!isset($this->nsqdTCPAddrsConnNum[$key])) {
                    $this->nsqdTCPAddrsConnNum[$key] = 0;
                }
                $partitionNode[$partition] = $key;
                if (!isset($nodePartitions[$key])) {
                    $nodePartitions[$key] = [];
                }
                $nodePartitions[$key][]= $partition;
            }
        }
        $this->partitionNode = $partitionNode;
        $this->nodePartitions = $nodePartitions;
        return $nodes;
    }

    /**
     * ConnectToNSQD takes a nsqd address to connect directly to.
     * It is recommended to use ConnectToNSQLookupd so that topics are discovered
     * automatically.  This method is useful when you want to connect to a single, local, instance.
     * @param string $host
     * @param string $port
     * @return mixed yield List<Connection>
     * @throws NsqException
     * @throws \Exception
     * @throws \Throwable
     */
    public function connectToNSQD($host, $port)
    {
        if ($this->isStopped) {
            throw new NsqException("consumer stopped");
        }

        $addr = "$host:$port";
        if (!isset($this->nsqdTCPAddrsConnNum[$addr])) {
            $this->nsqdTCPAddrsConnNum[$addr] = 0;
        }

        $perNsqdMaxNum = $this->getMaxConnNumPerNsqd();
        $currentNum = $this->nsqdTCPAddrsConnNum[$addr];
        $remainNum = max($perNsqdMaxNum - $currentNum, 0);

        $nsqdConns = [];
        for ($i = 0; $i < $remainNum; $i++) {
            try {
                $this->nsqdTCPAddrsConnNum[$addr]++;
                
                $conn = new Connection($host, $port);
                if ($this->rw == self::R) {
                    $conn->setExtraIdentifyParams($this->extraIdentifyParams);
                }
                if (isset($this->nodePartitions[$addr])) {
                    // TODO: specified partition
                    $pkey = array_rand($this->nodePartitions[$addr]);
                    $partition = $this->nodePartitions[$addr][$pkey];
                    $conn->setPartition($partition);
                }
                $conn->setExtendSupport($this->extendSupport);
                $hash = spl_object_hash($conn);
                
                try {
                    $this->pendingConnections[$hash] = $conn;
                    yield $conn->connect();
                    $this->idleConnections[$hash] = $conn;
                } finally {
                    unset($this->pendingConnections[$hash]);
                }

                $nsqdConns[] = $conn;
            } catch (\Throwable $t) {
                $this->nsqdTCPAddrsConnNum[$addr]--;
                throw $t;
            } catch (\Exception $ex) {
                $this->nsqdTCPAddrsConnNum[$addr]--;
                throw $ex;
            }
        }
        yield $nsqdConns;
    }

    /**
     * DisconnectFromNSQD closes the connection to and removes the specified
     * `nsqd` address from the list
     * @param string $host
     * @param int $port
     * @return void
     * @throws NsqException
     */
    public function disconnectFromNSQD($host, $port)
    {
        $key = "$host:$port";
        if (!isset($this->nsqdTCPAddrsConnNum[$key])) {
            throw new NsqException("not connected");
        }

        foreach ($this->getConnections() as $conn) {
            if ($conn->getHost() === $host && $conn->getPort() === $port) {
                $conn->tryClose();
            }
        }
        $this->nsqdTCPAddrsConnNum[$key] = 0;
    }

    public function disconnectFromNSQDConn(Connection $conn)
    {
        $hash = spl_object_hash($conn);
        if (isset($this->pendingConnections[$hash])) {
            $this->pendingConnections[$hash]->tryClose();
        }
        if (isset($this->idleConnections[$hash])) {
            $this->idleConnections[$hash]->tryClose();
        }
        if (isset($this->busyConnections[$hash])) {
            $this->busyConnections[$hash]->tryClose();
        }
        $this->nsqdTCPAddrsConnNum[$conn->getAddr()]--;
    }

    public function reconnect(Connection $conn)
    {
        if ($conn->isDisposable()) {
            yield false;
            return;
        }

        $hash = spl_object_hash($conn);
        //$addr = $conn->getLookupAddr();

        $reconnect = isset($this->idleConnections[$hash]) || isset($this->busyConnections[$hash]);
        //$isQueryLookupd = isset($this->lookupdHTTPAddrs[$addr]) || ($addr === null && count($this->lookupdHTTPAddrs) > 0);
        $isQueryLookupd = count($this->lookupdHTTPAddrs) > 0;
        if ($isQueryLookupd) {
            try {
                // trigger a poll of the lookupd
                yield $this->queryLookupd();
                yield true;
            } catch (\Throwable $t) {
                sys_echo("({$conn->getAddr()}) error reconnecting to nsqd - {$t->getMessage()}");
                yield false;
            } catch (\Exception $ex) {
                sys_echo("({$conn->getAddr()}) error reconnecting to nsqd - {$ex->getMessage()}");
                yield false;
            }
        } else if ($reconnect) {
            // there are no lookupd and we still have this nsqd TCP address in our list...
            // try to reconnect after a bit
            $this->delayReconnect($conn);
            yield false;
        }
    }

    private function delayReconnect(Connection $conn)
    {
        Timer::after(NsqConfig::getLookupdPollInterval(), function() use($conn) {
            if ($this->isStopped) {
                return;
            }
            $hash = spl_object_hash($conn);
            $reconnect = isset($this->idleConnections[$hash]) || isset($this->busyConnections[$hash]);
            if (!$reconnect) {
                return;
            }

            try {
                Task::execute($this->connectToNSQD($conn->getHost(), $conn->getPort()));
            } catch (\Throwable $t) {
                sys_echo("({$conn->getAddr()}) error reconnecting to nsqd - {$t->getMessage()}");
            } catch (\Exception $ex) {
                sys_echo("({$conn->getAddr()}) error reconnecting to nsqd - {$ex->getMessage()}");
            }
        });
    }

    /**
     * @return \Generator
     * @throws NsqException
     */
    public function take()
    {
        if (empty($this->nsqdTCPAddrsConnNum)) {
            throw new NsqException("no nsqd address found");
        }

        if (empty($this->idleConnections)) {
            goto disposableConn;
        }

        $key = array_rand($this->idleConnections);
        $conn = $this->idleConnections[$key];
        if ($conn->tryTake()) {
            $this->busyConnections[$key] = $conn;
            unset($this->idleConnections[$key]);
            yield [$conn];
        } else {
            disposableConn:
            list($host, $port) = explode(":", array_rand($this->nsqdTCPAddrsConnNum));
            list($conn) = (yield Connection::getDisposable($host, $port, NsqConfig::getDisposableConnLifecycle()));
            if ($this->delegate) {
                $this->delegate->onConnect($conn);
            }
            yield [$conn];
        }
    }

    public function release(Connection $conn)
    {
        $key = spl_object_hash($conn);
        if ($conn->tryRelease() && !$conn->isDisposable())  {
            unset($this->busyConnections[$key]);
            $this->idleConnections[$key] = $conn;
            return true;
        }
        return false;
    }

    /**
     * @return Connection[]
     */
    public function getNSQDConnections()
    {
        return $this->idleConnections + $this->busyConnections;
    }

    public function getNSQDAddrConnNum()
    {
        return $this->nsqdTCPAddrsConnNum;
    }

    public function getLookupdHTTPAddrs()
    {
        return $this->lookupdHTTPAddrs;
    }

    public function stats()
    {
        $nsqdConn = $this->getNSQDAddrConnNum();
        $lookupds = $this->getLookupdHTTPAddrs();

        /* @var array $nsqds */
        foreach ($lookupds as $url => $nsqds) {
            $ret = [];
            foreach ($nsqds as $i => $nsqd) {
                $addr = implode(":", $nsqd);
                $ret[$addr] = $nsqdConn[$addr];
            }
            $lookupds[$url] = $ret;
        }

        return  [
            "nsqdConnections_" => count($this->getNSQDConnections()),
            "lookupNsqConnections" => $lookupds,
        ];
    }

    public function removeConnection(Connection $conn)
    {
        $key = spl_object_hash($conn);
        unset($this->pendingConnections[$key]);
        unset($this->idleConnections[$key]);
        unset($this->busyConnections[$key]);
        $this->nsqdTCPAddrsConnNum[$conn->getAddr()]--;
    }

    public function stop()
    {
        $this->isStopped = true;
        Timer::clearTickJob($this->lookupdPollingTickId());
    }

    public function isStopped()
    {
        return $this->isStopped;
    }

    public function getTopic()
    {
        return $this->topic;
    }

    private function getConnections()
    {
        return $this->pendingConnections + $this->busyConnections + $this->idleConnections;
    }

    private function formatAddress($addr)
    {
        $scheme = parse_url($addr, PHP_URL_SCHEME);
        $host = parse_url($addr, PHP_URL_HOST);
        $port = parse_url($addr, PHP_URL_PORT) ?: 80;
        if (!$scheme || !$host) {
            throw new NsqException("invalid address $addr");
        }
        $host = strtolower($host);
        return "$scheme://$host:$port";
    }

    /**
     * (i + 1) mod n
     * @return string
     */
    private function nextLookupdEndpoint()
    {
        $num = count($this->lookupdHTTPAddrs);
        if ($num === 0) {
            return null;
        }

        if ($this->lookupdQueryIndex >= $num) {
            $this->lookupdQueryIndex = 0;
        }

        $address = array_keys($this->lookupdHTTPAddrs)[$this->lookupdQueryIndex];
        $this->lookupdQueryIndex = ($this->lookupdQueryIndex + 1) % $num;
        return $address;
    }

    /**
     * @return int
     */
    private function getMaxConnNumPerNsqd()
    {
        $nsqdNum = count($this->nsqdTCPAddrsConnNum);
        if ($nsqdNum === 0) {
            return 0;
        }
        return ceil($this->maxConnectionNum / $nsqdNum);
    }

    /**
     * @param string $addr
     * @param string $topic
     * @return \Generator
     */
    private function lookup($addr, $topic)
    {
        Command::checkTopicChannelName($topic);

        /* @var \Zan\Framework\Network\Common\Response $resp */
        $host = parse_url($addr, PHP_URL_HOST);
        $port = parse_url($addr, PHP_URL_PORT) ?: 80;

        // $host = (yield Dns::lookup($host, 1000));
        $httpClient = new HttpClient($host, $port);
        $params = ["topic" => $topic, 'metainfo' => 'true', 'access' => $this->rw];
        $resp = (yield $httpClient->get("/lookup", $params, NsqConfig::getNsqlookupdConnectTimeout()));
        yield static::validLookupdResp($resp);
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

    private function lookupdPollingTickId()
    {
        return spl_object_hash($this) . "_lookupd_polling_tick_id";
    }
}
