<?php
namespace SwooleSpider\Lib;

class SwooleServer
{
    protected $pid_file;
    public static $server;
    public static $channel;
    public static $worker_id;
    public static $config;
    static $SERVER_NAME;

    public function __construct($config)
    {
        self::$config = $config;
        self::$SERVER_NAME = $config['server_name']."-server";
        $this->pid_file = $config['pid_file'];
    }

    function onMasterStart(\swoole_server $server)
    {
        $this->setProcessName(self::$SERVER_NAME." master :".self::$config['host'].":".self::$config['port']);
        $this->log(self::$SERVER_NAME." onMasterStart start");
        file_put_contents($this->pid_file,self::$server->master_pid);
    }

    function onMasterShutdown(\swoole_server $server)
    {
        $this->log(self::$SERVER_NAME." onMasterShutdown");

        if (file_exists($this->pid_file))
        {
            $res = unlink($this->pid_file);
            if ($res)
                $this->log("unlink pidfile {$this->pid_file}");
        }
    }

    function onManagerStart(\swoole_server $server)
    {
        $this->setProcessName(self::$SERVER_NAME." manager");
    }

    function onManagerStop(\swoole_server $server)
    {
        $this->log(self::$SERVER_NAME." onManagerStop");
    }


    function onWorkerStart(\swoole_server $server, $worker_id)
    {
        $this->log(self::$SERVER_NAME." onWorkerStart,worker_id:".$worker_id);
        self::$worker_id = $worker_id;
        if (self::$worker_id <= self::$config['swoole']['worker_num'] -1)
        {
            $this->setProcessName(self::$SERVER_NAME." worker {$worker_id}");
        }
        else {
            $this->setProcessName(self::$SERVER_NAME." task worker {$worker_id}");
        }
    }

    function onWorkerStop(\swoole_server $server, $worker_id)
    {
        $this->log(" onWorkerStop,worker_id:".$worker_id);
    }

    function onReceive(\swoole_server $server, $fd, $from_id, $data)
    {
        $this->log(['op'=>'onReceive','data'=>$data,'from_id'=>$from_id,'fd'=>$fd]);
    }
    function onConnect(\swoole_server $server, $fd, $from_id)
    {
        $this->log(['op'=>'onConnect','from_id'=>$from_id,'fd'=>$fd]);
    }

    function onClose(\swoole_server $server, $fd, $from_id)
    {
        unset($this->_buffer[$fd]);
    }
    
    function onTask(\swoole_server $server,$task_id, $from_id, $params)
    {

    }

    function onFinish(\swoole_server $server, $task_id, $data)
    {
    }

    function create(){
        if (empty(self::$config['host'])) {

            $iplist = swoole_get_local_ip();
            $listenHost = "127.0.0.1";
            //监听局域网IP
            foreach ($iplist as $k => $v)
            {
                if (substr($v, 0, 7) == '192.168' or substr($v, 0, 6) == '172.16')
                {
                    $listenHost = $v;
                }
            }
            self::$config['host'] = $listenHost;
        }
        if (!empty(self::$config['host']) and !empty(self::$config['port']))
        {
            self::$server = new \swoole_server(self::$config['host'], self::$config['port'], SWOOLE_PROCESS, SWOOLE_SOCK_TCP);
            $this->log(self::$SERVER_NAME." config success,pid:".$this->pid_file);
        }
        else
        {
            $this->log(self::$SERVER_NAME." start failed");
            exit;
        }
    }

    function run($_setting = array())
    {
        $this->create();
        $setting = array_merge(self::$config['swoole'], $_setting);
        self::$server->set($setting);
        self::$server->on('start', array($this, 'onMasterStart'));
        self::$server->on('Shutdown', array($this, 'onMasterShutdown'));
        self::$server->on('managerStart', array($this, 'onManagerStart'));
        self::$server->on('managerStop', array($this, 'onManagerStop'));
        self::$server->on('workerStart', array($this, 'onWorkerStart'));
        self::$server->on('workerStop', array($this, 'onWorkerStop'));
        self::$server->on('Connect', array($this, 'onConnect'));
        self::$server->on('Receive', array($this, 'onReceive'));
        self::$server->on('Task', array($this, 'onTask'));
        self::$server->on('Close', array($this, 'onClose'));
        self::$server->on('Finish', array($this, 'onFinish'));
        self::$server->start();
    }

    function log($msg){
        $msg = is_array($msg)?json_encode($msg,256):$msg;
        echo date('Y-m-d H:i:s') ." ".self::$SERVER_NAME.  "  : $msg\n";
    }

    /**
     * 设置进程的名称
     * @param $name
     */
    function setProcessName($name)
    {
        if (function_exists('cli_set_process_title'))
        {
            @cli_set_process_title($name);
        }
        else
        {
            if (function_exists('swoole_set_process_name'))
            {
                @swoole_set_process_name($name);
            }
            else
            {
                trigger_error(__METHOD__ . " failed. require cli_set_process_title or swoole_set_process_name.");
            }
        }
    }

    /**
     * [cmd description]
     * @param  [type] $cmd [description]
     * @return [type]      [description]
     */
    function start($config=[]){
        global $argv; 
        if( isset($argv[1]) && $argv[1] ){
            $cmd = $argv[1];
        }else{
            goto usage;
        }
        $server_pid = file_exists($this->pid_file)?file_get_contents($this->pid_file):0;
        if ($cmd == 'reload') {
            if (empty($server_pid)) {
                exit("Server is not running.\n");
            }
            $status = posix_kill($server_pid, SIGUSR1);
            exit;
        } elseif ($cmd == 'stop') {
            if (empty($server_pid)) {
                exit("Server is not running.\n");
            }
            $status = posix_kill($server_pid, SIGTERM);
            exit("$server_pid Server stop end $status.\n");
        } elseif ($cmd == 'start') {
            //已存在ServerPID，并且进程存在
            if (!empty($server_pid) and posix_kill($server_pid, 0)) {
                exit("Server is already running.\n");
            }
        } else {
            usage:
            exit("Usage: php {$argv[0]} start|stop|reload\n");

        }
        $this->run($config);
    }
}
