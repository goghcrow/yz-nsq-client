<?php

/*
static void client_onReceive(swClient *cli, char *data, uint32_t length)
{
// ...
    zval *zobject = cli->object;
    if (NULL == zobject) {
        return;
    }

    zval *zcallback = NULL;
    zval **args[2];
    zval *retval;

    zval *zdata;
    SW_MAKE_STD_ZVAL(zdata); // 这里申请的zval的内存,引用计数已经为1
    SW_ZVAL_STRINGL(zdata, data, length, 1);

    args[0] = &zobject;
    args[1] = &zdata;
    sw_zval_add_ref(&zobject);
    sw_zval_add_ref(&zdata); // 这里又把引用计数+1, 变成了2
    client_callback *cb = swoole_get_property(zobject, 0);
    zcallback = cb->onReceive;
// ...
    if (retval != NULL)
    {
        sw_zval_ptr_dtor(&retval);
    }
    if (zobject != NULL) {
        sw_zval_ptr_dtor(&zobject);
    }

    free_zdata:
    if (zdata != NULL) {
        sw_zval_ptr_dtor(&zdata); // 这里释放 -1, 之后=1, 永远也不会释放了
    }
}
 */
// swoole memory leak
$getId = function() {
    $id = json_encode([
        "client_id" => gethostname(),
        "feature_negotiation" => true,
        "heartbeat_interval" => 60000,
    ]);
    return "IDENTIFY \n" . pack('N', strlen($id)) . $id;
};

$client = new \swoole_client(SWOOLE_SOCK_TCP, SWOOLE_SOCK_ASYNC);

$client->set([
    "open_length_check" => true,
    "package_length_type" => 'N',
    "package_length_offset" => 0,
    "package_body_offset" => 4,
    "open_tcp_nodelay" => true,
]);

$client->on("connect", function(\swoole_client $client) use($getId) {
    $client->send("  V2");
    $client->send($getId());
});

$i = 0;
$recv = 0;
register_shutdown_function(function() {
    global $recv;
    echo number_format($recv), " bytes\n";

});
$client->on("receive", function(\swoole_client $client, $bytes) {
    global $i, $recv;
    $recv += strlen($bytes);

    echo $bytes, "\n";
    if ($i === 0) {
        $client->send("SUB zan_mqworker_test ch1\n");
        $client->send("RDY 1\n");
    } else if ($i >= 2) {
        $client->send("FIN " . substr($bytes, 18, 16) . "\n");
    }
    $i++;
});
$client->on("error", function(\swoole_client $client) { echo "ERROR\n";});
$client->on("close", function(\swoole_client $client) { echo "CLOSE\n"; });

$client->connect("10.9.52.215", 4150);