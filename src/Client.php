<?php

namespace xiaohe\messagequeue;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class Client
{
    /**
     * @const DEFAULT_VHOST 默认的vhost
     */
    const DEFAULT_VHOST = '/';

    /**
     * @const DEFAULT_DELIVERY_MODEL
     * 交付时是否持久化，1非持久化，2持久化
     */
    const DEFAULT_DELIVERY_MODEL = 2;

    /**
     * @var $config 连接MQ的配置参数
     */
    private $config;
    private $connection;

    public function __construct($config)
    {
        $this->config = $config;
        $this->init();
    }

    private function init()
    {
        $this->check();

        if (!isset($this->config['vhost'])) {
            $this->config['vhost'] = self::DEFAULT_VHOST;
        }

        $this->connection = $this->createConnection();
    }

    private function check()
    {
        if (empty($this->config['host'])) {
            throw new Exception('create connection failed, host is empty');
        }
        if (!isset($this->config['port'])) {
            throw new Exception('create connection failed, port is empty');
        }
        if (!isset($this->config['username'])) {
            throw new Exception('create connection failed, username is empty');
        }
        if (!isset($this->config['password'])) {
            throw new Exception('create connection failed, password is empty');
        }
    }

    private function createConnection()
    {
        $connection = new AMQPStreamConnection(
            $this->config['host'],
            $this->config['port'],
            $this->config['username'],
            $this->config['password'],
            $this->config['vhostname']
        );

        return $connection;
    }

    /**
     * 发布
     * @params string $queue 队列名称
     * @params array $body 队列数据
     */
    public function publish($queue, $body)
    {
        $channel = $this->connection->channel();
        $channel->queue_declare($queue, false, true, false, false);
        $message = new \PhpAmqpLib\Message\AMQPMessage($body, array('delivery_mode' => self::DEFAULT_DELIVERY_MODEL));
        $channel->basic_publish($message);

        $channel->close();
        $this->connection->close();
    }

    /**
     * 消费
     * @param queue string 队列名称
     * @param $callback 消费之后的处理程序
     */
    public function consume($queue, $callback)
    {
        $channel = $this->connection->channel();
        $channel->queue_declare($queue, false, true, false, false);

        $channel->basic_qos(0, 1, false);
        $channel->basic_consume($queue, '', false, false, false, false, function($message) {
            $body = json_decode($message->body, true);
            $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
            if (isset($callback)) {
                call_user_func($callback, $body);
            }
        });
        while(count($channel->callbacks)) {
            $channel->wait();
        }

        $channel->close();
        $this->connection->close();
    }
}