<?php
namespace SwooleSpider\Queue;

class RedisQueue implements QueueInterface
{
    public $redis = null;
    public $config = [];
    public $maxQueueSize = 100000000;
    public $maxQueuedCount = 0;
    public $bloomFilter = false;

    protected $name = '';
    protected $key = '';
    protected $queuedKey = '';
    protected $algorithm = 'depth';

    protected $bfSize;
    protected $bfHashCount;

    public function __construct($config)
    {
        $this->config = $config;
        $this->name = $config['name'];
        $this->key = $config['name'] . 'Queue';
        $this->queuedKey = $config['name'] . 'Queued';
        $this->bfSize = isset($config['size']) ? $config['size'] : 400000000;
        $this->bfHashCount = isset($config['hash_count']) ? $config['hash_count'] : 14;
        if (isset($config['bloomFilter'])) {
            $this->bloomFilter = $config['bloomFilter']?true:false;
        }

        if (isset($config['algorithm'])) {
            $this->algorithm = $config['algorithm'] != 'breadth' ? 'depth' : 'breadth';
        }
        $this->getInstance()->sAdd('swooleSpider', $this->name);
    }

    public function getInstance()
    {
        if (!$this->redis) {
            $this->redis = new RedisServer($this->config);
            // $this->redis->pconnect($this->config['host'], $this->config['port']);
            // if($this->config['password']){
            //     $this->redis->auth($this->config['password']);
            // }
        }
        return $this->redis;
    }

    public function add($url, $options = [])
    {
        if (!$url || ($this->maxQueueSize != 0 && $this->count() >= $this->maxQueueSize)) {
            return false;
        }

        $queue = serialize([
            'url' => $url,
            'options' => $options,
        ]);

        if ($this->isQueued($queue)) {
            return false;
        }

        $this->getInstance()->rPush($this->key, $queue);
        return true;
    }

    public function checkQueue($url, $options = []){
        $queue = serialize([
            'url' => $url,
            'options' => $options,
        ]);
        return $this->isQueued($queue);
    }

    public function next()
    {
        if ($this->algorithm == 'depth') {
            $queue = $this->getInstance()->lPop($this->key);
        } else {
            $queue = $this->getInstance()->rPop($this->key);
        }

        if ($this->isQueued($queue)) {
            return $this->next();
        } else {
            return @unserialize($queue);
        }
    }

    public function count()
    {
        return $this->getInstance()->lSize($this->key);
    }

    public function queued($queue)
    {
        if ($this->bloomFilter) {
            $this->bfAdd(md5(serialize($queue)));
        } else {
            $this->getInstance()->sAdd($this->queuedKey, md5(serialize($queue)));
        }
    }

    public function isQueued($queue)
    {
        $queue = is_array($queue)?serialize($queue):$queue;
        if ($this->bloomFilter) {
            return $this->bfHas(md5($queue));
        } else {
            return $this->getInstance()->sIsMember($this->queuedKey, md5($queue));
        }
    }

    public function queuedCount()
    {
        if ($this->bloomFilter) {
            return 0;
        } else {
            return $this->getInstance()->sSize($this->queuedKey);
        }
    }

    public function clean()
    {
        $this->getInstance()->delete($this->key);
        $this->getInstance()->delete($this->queuedKey);
        $this->getInstance()->sRem('swooleSpider', $this->name);
    }

    protected function bfAdd($item)
    {
        $index = 0;
        $pipe = $this->getInstance()->pipeline();
        while ($index < $this->bfHashCount) {
            $crc = $this->hash($item, $index);
            $pipe->setbit($this->queuedKey, $crc, 1);
            $index++;
        }
        $pipe->exec();
    }

    protected function bfHas($item)
    {
        $index = 0;
        $pipe = $this->getInstance()->pipeline();
        while ($index < $this->bfHashCount) {
            $crc = $this->hash($item, $index);
            $pipe->getbit($this->queuedKey, $crc);
            $index++;
        }
        $result = $pipe->exec();
        return !in_array(0, $result);
    }

    protected function hash($item, $index)
    {
        return abs(crc32(md5('m' . $index . $item))) % $this->bfSize;
    }
}
class RedisServer
{
    public $_redis;
    public $config;

    public static $prefix = "autoinc_key:";

    function __construct($config)
    {
        $this->config = $config;
        $this->connect();
    }

    function connect()
    {
        try
        {
            if ($this->_redis)
            {
                unset($this->_redis);
            }
            $this->_redis = new \Redis();
            if ($this->config['pconnect'])
            {
                $this->_redis->pconnect($this->config['host'], $this->config['port'], $this->config['timeout']);
            }
            else
            {
                $this->_redis->connect($this->config['host'], $this->config['port'], $this->config['timeout']);
            }
            
            if (!empty($this->config['password']))
            {
                $this->_redis->auth($this->config['password']);
            }
            if (!empty($this->config['database']))
            {
                $this->_redis->select($this->config['database']);
            }
        }
        catch (\RedisException $e)
        {
            $this->log(__CLASS__ . " Swoole Redis Exception" . var_export($e, 1));
            return false;
        }
    }

    function __call($method, $args = array())
    {
        $reConnect = false;
        while (1)
        {
            try
            {
                $result = call_user_func_array(array($this->_redis, $method), $args);
            }
            catch (\RedisException $e)
            {
                //已重连过，仍然报错
                if ($reConnect)
                {
                    throw $e;
                }

                $this->log(__CLASS__ . " [" . posix_getpid() . "] Swoole Redis[{$this->config['host']}:{$this->config['port']}]
                 Exception(Msg=" . $e->getMessage() . ", Code=" . $e->getCode() . "), Redis->{$method}, Params=" . var_export($args, 1));
                $this->_redis->close();
                $this->connect();
                $reConnect = true;
                continue;
            }
            return $result;
        }
        //不可能到这里
        return false;
    }


    function log($msg){
        $msg = is_array($msg)?json_encode($msg,1):$msg;
        echo date('Y-m-d H:i:s') ." RedisServer  : $msg\n";
    }

}