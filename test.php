<?php
require_once __DIR__.'/src/autoload.php';
require_once __DIR__.'/vendor/autoload.php';

define('SPIDER_PATH',"/data/logs/chezhu-service/spider");
define('SPIDER_NAME',"spider");
$redisconf = array(
    'host'    => '127.0.0.1',
    'port'    => 6379,
    'password' => '',
    'timeout' => 0.5,
    'pconnect' => true,
    'name'=>'spider_queue'
);

$sp = new \SwooleSpider\Spider();
$sp->daemonize = true;
$sp->name = SPIDER_NAME;
$sp->count = 1;
$sp->interval = 1000;
$sp->timeout = 100;

// $sp->urlFilter = $config['SPIDER_URL_Filter'];
$sp->logFile = SPIDER_PATH . '/'.SPIDER_NAME.'.log';
$sp->pidFile = SPIDER_PATH . '/'.SPIDER_NAME.'.pid';
$sp->setQueue('redis',$redisconf);
$sp->add('http://baidu.com',['tag'=>time()]);
// $sp->seed[] = 'https://car.autohome.com.cn/mtn/series/cycle/27703';
// $sp->seed[] = ['https://car.autohome.com.cn/mtn/series/cycle/27703',[
//  'tag'=>'autohome_spider',
//  'method'=>'get',
//  'car_model_id'=>'27703',
//     'callbackFunc' => [
//      'func' => "\Spider\AutoHome::BaoYangProcess",
//  ],
// ]];

// $sp->beforeDownloadPage = function ($spider) {
//     // 在爬取前设置请求的 headers 
//     $sp->options['headers'] = [
//         // 'Host' => 'www.zhihu.com',
//         'Connection' => 'keep-alive',
//         'Cache-Control' => 'max-age=0',
//         'Upgrade-Insecure-Requests' => '1',
//         // 'User-Agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
//         // 'Accept' => 'application/json, text/plain, */*',
//         // 'Accept-Encoding' => 'gzip, deflate, sdch, br',
//         // 'authorization' => 'oauth c3cef7c66a1843f8b3a9e6a1e3160e20',
//     ];
// };

// $sp->downloadPage = function($spider){
//     $worker_id = isset($spider->id) ? $spider->id : '';
//     // echo date("c"),"\t","downloadPage ",$sp->url,"\n";
//     // $time_start = microtime_float();
//     $spider->log("Beanbun worker {$worker_id} download {$spider->url} start.");
//     // $urlmd5 = md5($sp->url);
//     // $file = SPIDER_PATH . '/data/' . $urlmd5;
//     // $use_cache = 0;
//     // if( file_exists($file) ){
//     // // if( file_exists($file) && ($ftime = filectime($file)) && $ftime+43200 > time()  ){
//     //     $sp->page = file_get_contents($file);
//     //     $use_cache = 1;
//     // }else{
//     //     unset($sp->options['callbackFunc']);
//     //     unset($sp->options['tag']);
//     //     unset($sp->options['car_model_id']);
//     //     $response = $sp->downloader()->request($sp->method, $sp->url, $sp->options);
//     //     $sp->page = $response->getBody();
//     // }
//     // $time_end = microtime_float();
//     // $time = number_format($time_end - $time_start,4);
//     // if ($sp->page) {
//     //     $sp->log("Beanbun worker {$worker_id} download $time {$sp->url} use_cache $use_cache success.");
//     // } else {
//     //     $sp->log("Beanbun worker {$worker_id} download $time {$sp->url} error.");
//     //     $sp->error();
//     // }
// };

$sp->afterDownloadPage = function ($spider) {
    // try {
    echo date("c"),"\t","afterDownloadPage ",json_encode($spider->queue,1),"\n";
    $spider->page = (string) $spider->page;
    $spider->log(json_encode($spider->page,256));
    //     if (strlen($sp->page) < 100) {
    //         $sp->queueAdd($sp->url,$sp->queue['options']);
    //         $sp->error();
    //     }
    //     $urlmd5 = md5($sp->url);
    //     $file = SPIDER_PATH . '/data/' . $urlmd5;
    //     if( !file_exists($file) ) file_put_contents($file, $sp->page);
    //     $content = file_get_contents($file);
    //     $sp->document = new \DiDom\Document($content);
    //     //设置完成回调
    //     if( isset($sp->queue['options']['callbackFunc']) && isset($sp->queue['options']['callbackFunc']['func'])  ){
    //         if (!is_callable($sp->queue['options']['callbackFunc']['func']))
    //         {
    //             return false;
    //         }
    //         //调用callbackFunc方法
    //         $ret = call_user_func($sp->queue['options']['callbackFunc']['func'], $spider);
    //         _Log("callbackFunc :".$sp->queue['options']['callbackFunc']['func']." execution succeed,result:".(is_array($ret)?json_encode($ret,256):$ret));
    //     }
    //     $sp->queue()->queued($sp->queue);
    // } catch (Exception $e) {
    //     $sp->queue()->queued($sp->queue);
    //     _Log("Exception error :".$e->getMessage(),4);
    // }
};
// 不需要框架来发现新的网址，
$sp->discoverUrl = function () {};
$sp->start();