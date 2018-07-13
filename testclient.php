<?php
$client = new swoole_client(SWOOLE_SOCK_TCP, SWOOLE_SOCK_ASYNC);
$client->set(array(
    'open_length_check' => true,
    'package_max_length' => 2097152,
    'package_length_type' => 'N',
    'package_body_offset' => 16,
    'package_length_offset' => 0,
));

$client->on("connect", function($cli) {
    $cli->send("hello world\n");
});

$client->on("receive", function($cli, $data) {
        echo "received: $data\n";
        sleep(1);
        $cli->send("hello\n");
});

$client->on("close", function($cli){
    echo "closed\n";
});

$client->on("error", function($cli){
    exit("error\n");
});

$client->connect('127.0.0.1', 19999, 0.5);
$client->send("hello\n");
