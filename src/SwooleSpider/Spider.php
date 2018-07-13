<?php
namespace SwooleSpider;

use SwooleSpider\Exception\SwooleSpiderException;
use SwooleSpider\Lib\Helper;
use SwooleSpider\Lib\SwooleServer;
use Exception;
use GuzzleHttp\Client;

class Spider
{
    const VERSION = '1.0.0';

    public $id = null;
    public $name = null;
    public $max = 0;
    public $seed = [];
    public $daemonize = true;
    public $urlFilter = [];
    public $interval = 5;
    public $timeout = 5;
    public $userAgent = 'pc';
    public $logFile = '';
    public $errLogFile = '';
    public $pidFile = '';
    public $commands = [];

    public $queue = '';
    public $url = '';
    public $method = '';
    public $options = [];
    public $page = '';

    public $startWorker = '';
    public $beforeDownloadPage = '';
    public $downloadPage = '';
    public $afterDownloadPage = '';
    public $discoverUrl = '';
    public $afterDiscover = '';
    public $stopWorker = '';
    public $exceptionHandler = '';

    public $hooks = [
        'startWorkerHooks',
        'beforeDownloadPageHooks',
        'downloadPageHooks',
        'afterDownloadPageHooks',
        'discoverUrlHooks',
        'afterDiscoverHooks',
        'stopWorkerHooks',
    ];
    public $startWorkerHooks = [];
    public $beforeDownloadPageHooks = [];
    public $downloadPageHooks = [];
    public $afterDownloadPageHooks = [];
    public $discoverUrlHooks = [];
    public $afterDiscoverHooks = [];
    public $stopWorkerHooks = [];
    public $enSwoole = true;

    protected $queues = null;
    protected $downloader = null;
    public static $worker = null;
    public static $server = null;
    protected $timer_id = null;
    protected $queueFactory = null;
    protected $queueArgs = [];
    protected $downloaderFactory = null;
    protected $downloaderArgs = [];
    protected $logFactory = null;

    public static function timer($interval, $callback, $args = [])
    {
        return \swoole_timer_tick($interval,$callback,$args);
    }

    public static function timerDel($time_id)
    {
        \swoole_timer_clear($time_id);
    }

    public function run()
    {
        self::$worker->run();
    }

    public function __construct($config = [])
    {
        global $argv;
        $this->commands = $argv;
        $this->name = isset($config['name'])
        ? $config['name']
        : current(explode('.', $this->commands[0]));
        $this->logFile = isset($config['logFile']) ? $config['logFile'] : __DIR__ . '/' . $this->name . '_access.log';
        $this->errLogFile = isset($config['logFile']) ? $config['logFile'] : __DIR__ . '/' . $this->name . '_error.log';
        $this->pidFile = isset($config['logFile']) ? $config['logFile'] : __DIR__ . '/' . $this->name . '.pid';
        $this->setQueue();
        $this->setDownloader();
        $this->setLog();
    }

    public function command()
    {
        switch ($this->commands[1]) {
            case 'start':
                foreach ((array) $this->seed as $url) {
                    if (is_string($url)) {
                        $this->queue()->add($url);
                    } elseif (is_array($url)) {
                        $this->queue()->add($url[0], $url[1]);
                    }
                }
                $this->queues = null;
                echo "SwooleSpider is starting...\n";
                // $STDOUT = fopen($this->logFile, "a");
                break;
            case 'clean':
                $this->queue()->clean();
                unlink($this->logFile);
                die();
                break;
            case 'stop':
                break;
            default:
                break;
        }
    }

    // 执行爬虫
    public function start()
    {
        // if (!isset($this->commands[1])) {
        //     $this->daemonize = false;
        // }

        if ($this->daemonize || $this->enSwoole ) {
            $this->check();

            $config = [
                'host'=> '127.0.0.1',
                'port'=> '19999',
                'server_name'=> $this->name,
                'pid_file'=> $this->pidFile,
                "swoole" => array(
                    "log_file" => $this->logFile,
                    'worker_num' => $this->count,
                    // "task_worker_num" => 64,
                    'dispatch_mode' => 3,
                    'max_request' => 3000,
                    'daemonize' => $this->daemonize,
                    'open_length_check' => 1,
                    'package_max_length' => 2465792, //2M默认最大长度
                    'package_length_type' => 'N',
                    'package_body_offset' => 16,
                    'package_length_offset' => 0,
                ),
                "php" => array(//php行为配置
                    "error_log" => $this->errLogFile,
                    //可以新增其他配置
                ),
                "transaction_expire_time"=>10000,
            ];

            // $worker = new \SwooleSpider\Lib\SwooleServer($config);
            $worker = new Server($config);
            Server::$serverP['onWorkerStart'] = [$this, 'onWorkerStart'];
            Server::$serverP['onWorkerStop'] = [$this, 'onWorkerStop'];
            self::$worker = $worker;
            // self::$server = Server::$server;
            empty($this->queueArgs['name']) and $this->queueArgs['name'] = $this->name;
            $this->initHooks();
            $this->command();
            $worker->start();

        } else {
            $this->initHooks();
            $this->seed = (array) $this->seed;
            while (count($this->seed)) {
                $this->crawler();
            }
        }
    }

    public function check()
    {
        $error = false;
        $text = '';
        $version_ok = $pcntl_loaded = $posix_loaded = true;
        if (!version_compare(phpversion(), "5.6.0", ">=")) {
            $text .= "PHP Version >= 5.6.0                 \033[31;40m [fail] \033[0m\n";
            $error = true;
        }

        if ($error) {
            echo $text;
            exit;
        }
    }

    public function initHooks()
    {
        $this->startWorkerHooks[] = function ($SwooleSpider) {
            // $SwooleSpider->id = $SwooleSpider->worker->id;
            $SwooleSpider->log("SwooleSpider worker {$SwooleSpider->id} is starting ...");
        };

        if ($this->startWorker) {
            $this->startWorkerHooks[] = $this->startWorker;
        }

        $this->startWorkerHooks[] = function ($SwooleSpider) {
            $SwooleSpider->queue()->maxQueueSize = $SwooleSpider->max;
            $SwooleSpider->timer_id = Spider::timer($SwooleSpider->interval, [$SwooleSpider, 'crawler']);
        };

        $this->beforeDownloadPageHooks[] = [$this, 'defaultBeforeDownloadPage'];

        if ($this->beforeDownloadPage) {
            $this->beforeDownloadPageHooks[] = $this->beforeDownloadPage;
        }

        if ($this->downloadPage) {
            $this->downloadPageHooks[] = $this->downloadPage;
        } else {
            $this->downloadPageHooks[] = [$this, 'defaultDownloadPage'];
        }

        if ($this->afterDownloadPage) {
            $this->afterDownloadPageHooks[] = $this->afterDownloadPage;
        }

        if ($this->discoverUrl) {
            $this->discoverUrlHooks[] = $this->discoverUrl;
        } elseif ($this->daemonize) {
            $this->discoverUrlHooks[] = [$this, 'defaultDiscoverUrl'];
        }

        if ($this->afterDiscover) {
            $this->afterDiscoverHooks[] = $this->afterDiscover;
        }

        if ($this->daemonize) {
            $this->afterDiscoverHooks[] = function ($SwooleSpider) {
                $SwooleSpider->queue()->queued($SwooleSpider->queue);
            };
        }

        if ($this->stopWorker) {
            $this->stopWorkerHooks[] = $this->stopWorker;
        }

        if (!$this->exceptionHandler) {
            $this->exceptionHandler = [$this, 'defaultExceptionHandler'];
        }
    }

    // 爬虫进程
    public function onWorkerStart($worker)
    {
        // print_r($worker);
        foreach ($this->startWorkerHooks as $hook) {
            $this->id = $worker->id;
            call_user_func($hook, $this);
        }
    }

    public function queue()
    {
        if ($this->queues == null) {
            $this->queues = call_user_func($this->queueFactory, $this->queueArgs);
        }
        return $this->queues;
    }

    public function add($url,$options=[]){
        $this->seed[] = [$url,$options];
    }

    public function queueAdd($url,$options=[]){
        if (count($this->urlFilter) > 0) {
            foreach ($this->urlFilter as $urlPattern) {
                if (preg_match($urlPattern, $url)) {
                    $this->queue()->add($url,$options);
                }
            }
        } else {
            $this->queue()->add($url,$options);
        }
    }

    public function queued(){

    }

    public function setQueue($callback = null, $args = [
        'host' => '127.0.0.1',
        'port' => '2207',
    ]) {
        if ($callback === 'memory' || $callback === null) {
            $this->queueFactory = function ($args) {
                return new \SwooleSpider\Queue\MemoryQueue($args);
            };
        } elseif ($callback == 'redis') {
            $this->queueFactory = function ($args) {
                return new \SwooleSpider\Queue\RedisQueue($args);
            };
        } else {
            $this->queueFactory = $callback;
        }

        $this->queueArgs = $args;
    }

    public function downloader()
    {
        if ($this->downloader === null) {
            $this->downloader = call_user_func($this->downloaderFactory, $this->downloaderArgs);
        }
        return $this->downloader;
    }

    public function setDownloader($callback = null, $args = [])
    {
        if ($callback === null) {
            $this->downloaderFactory = function ($args) {
                return new Client($args);
            };
        } else {
            $this->downloaderFactory = $callback;
        }
        $this->downloaderArgs = $args;
    }

    public function log($msg)
    {
        call_user_func($this->logFactory, $msg, $this);
    }

    public function setLog($callback = null)
    {
        $this->logFactory = $callback === null
        ? function ($msg, $SwooleSpider) {
            echo date('Y-m-d H:i:s') . " {$SwooleSpider->name} : $msg\n";
        }
        : $callback;
    }

    public function error($msg = null)
    {
        throw new SwooleSpiderException($msg);
    }

    public function crawler()
    {
        try {
            if(time()%10===0) $this->log("{$this->name} : crawler start");
            $allHooks = $this->hooks;
            array_shift($allHooks);
            array_pop($allHooks);

            foreach ($allHooks as $hooks) {
                foreach ($this->$hooks as $hook) {
                    call_user_func($hook, $this);
                }
            }
            if(time()%10===0) $this->log("{$this->name} : crawler end");
        } catch (Exception $e) {
            call_user_func($this->exceptionHandler, $e);
        }

        $this->queue = '';
        $this->url = '';
        $this->method = '';
        $this->page = '';
        $this->options = [];
    }

    public function onWorkerStop($worker)
    {
        foreach ($this->stopWorkerHooks as $hook) {
            call_user_func($hook, $this);
        }
    }

    public function defaultExceptionHandler(Exception $e)
    {
        if ($e instanceof SwooleSpiderException) {
            if ($e->getMessage()) {
                $this->log($e->getMessage());
            }
        } elseif ($e instanceof Exception) {
            $this->log($e->getMessage());
            if ($this->daemonize) {
                $this->queueAdd($this->queue['url'], $this->queue['options']);
            } else {
                $this->seed[] = $this->queue;
            }
        }
    }

    public function defaultBeforeDownloadPage()
    {
        if(time()%10===0) $this->log("BeforeDownloadPage start");
        if ($this->daemonize) {
            if ($this->max > 0 && $this->queue()->queuedCount() >= $this->max) {
                $this->log("Download to the upper limit, SwooleSpider worker {$this->id} stop downloading.");
                self::timerDel($this->timer_id);
                $this->error();
            }

            $this->queue = $queue = $this->queue()->next();
        } else {
            $this->queue = $queue = array_shift($this->seed);
            if( is_array($queue) && isset($queue[0]) && isset($queue[1]) ){
                $this->queue = $queue = ['url'=>$queue[0],'options'=>$queue[1]];
            }
        }

        $this->log("BeforeDownloadPage queue:".json_encode($queue,256));

        if (is_null($queue) || !$queue) {
            sleep(30);
            $this->error();
        }

        if (!is_array($queue)) {
            $this->queue = $queue = [
                'url' => $queue,
                'options' => [],
            ];
        }

        $options = array_merge([
            'headers' => [],
            'reserve' => true,
            'timeout' => $this->timeout,
        ], (array) $queue['options']);

        if ($this->daemonize && $options['reserve'] && $this->queue()->isQueued($queue)) {
            $this->error();
        }

        $this->url = $queue['url'];
        $this->method = isset($options['method']) ? $options['method'] : 'GET';
        $this->options = $options;
        if (!isset($this->options['headers']['User-Agent'])) {
            $this->options['headers']['User-Agent'] = Helper::randUserAgent($this->userAgent);
        }
        if(time()%10===0) $this->log("BeforeDownloadPage end");
    }

    public function defaultDownloadPage()
    {
        $worker_id = isset($this->id) ? $this->id : '';
        $this->log("SwooleSpider worker {$worker_id} download {$this->url} start.");
        unset($this->options['callbackFunc']);
        unset($this->options['tag']);
        $response = $this->downloader()->request($this->method, $this->url, $this->options);
        $this->page = $response->getBody();
        if ($this->page) {
            $this->log("SwooleSpider worker {$worker_id} download {$this->url} success.");
        } else {
            $this->error();
        }
    }

    public function defaultDiscoverUrl()
    {
        $countUrlFilter = count($this->urlFilter);
        if ($countUrlFilter === 1 && !$this->urlFilter[0]) {
            $this->error();
        }

        $urls = Helper::getUrlByHtml($this->page, $this->url);

        if ($countUrlFilter > 0) {
            foreach ($urls as $url) {
                foreach ($this->urlFilter as $urlPattern) {
                    if (preg_match($urlPattern, $url)) {
                        $this->queue()->add($url);
                    }
                }
            }
        } else {
            foreach ($urls as $url) {
                $this->queue()->add($url);
            }
        }
    }

    public function middleware($middleware, $action = 'handle')
    {
        if (is_object($middleware)) {
            $middleware->$action($this);
        } else {
            call_user_func($middleware, $this);
        }
    }
}
class Server extends SwooleServer
{
    public static $serverP;
    public function __construct($config)
    {
        parent::__construct($config);
    }

    function onWorkerStart(\swoole_server $server, $worker_id)
    {
        parent::onWorkerStart($server, $worker_id);
        if( isset(self::$serverP['onWorkerStart']) ){
            self::$serverP['onWorkerStart'][0]->id = $worker_id;
            call_user_func(self::$serverP['onWorkerStart'],self::$serverP['onWorkerStart'][0]);
        }
    }

    function onWorkerStop(\swoole_server $server, $worker_id)
    {
        parent::onWorkerStop($server, $worker_id);
        if( isset(self::$serverP['onWorkerStop']) ){
            self::$serverP['onWorkerStop'][0]->id = $worker_id;
            call_user_func(self::$serverP['onWorkerStop'],self::$serverP['onWorkerStop'][0]);
        }
    }

    function __call($method, $args = array())
    {
        $this->log("__call:$method");
        $result = call_user_func_array(array($this, $method), $args);
        if( isset(self::$serverP[$method]) ){
            $result = call_user_func(self::$serverP[$method],self::$serverP[$method][0]);
        }        
        return $result;
    }
}

