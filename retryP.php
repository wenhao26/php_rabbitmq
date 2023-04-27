<?php
/**
 * 使用版本 composer require php-amqplib/php-amqplib 3.5
 * 下载 扩展 amqp
 * 下载需要开启 php.ini 中的开启 extension=sockets
 * demo 参考 https://ziruchu.com/art/530
 */

require_once('vendor/autoload.php');

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class retryP
{
    private $mqConnection;
    private $channel;

    public function __construct()
    {
        // 1、建立连接
        $this->mqConnection = new AMQPStreamConnection('localhost', 5672, 'guest', 'guest');
        // 2、创建通道
        $this->channel = $this->mqConnection->channel();
    }

    public function logs($log)
    {
        $date = date("Y-m-d");
        $writable_path = 'logs';
        if (!file_exists($writable_path)) {
            mkdir($writable_path, 0777);
        }
        $day_log = $writable_path . '/' . $date . '.log';
        file_put_contents($day_log, $log . PHP_EOL, FILE_APPEND | LOCK_EX);
    }

    // 简单模式：生产者
    public function simpleMq()
    {
        // 3、创建队列
        /**
         * $queue    ''    队列名称（唯一）
         * $passive    false    被动模式。为 true 时，如时 $queue 不存在，则返回错误（不创建队列，只检测队列是否存在）；为 false 时，若 $queue 不存在，则创建此队列，然后返回 OK
         * $durable    false    队列持久化。为 true 时，消费将存入数据库，即使服务崩溃，消息也不会消失
         * $exclusive    false    排他性。为 true 时，只能在本次连接中使用，连接关闭时自动消亡（即使 $durable 为 true）
         * $auto_delete    true    自动消亡。为 true 时，当队列不再有订阅者时，会自动消亡
         * $nowait    false    异步执行。为 true 时，不等待队列创建结果，立即完成函数调用
         * $arguments    array    设置消息队列的额外参数，如存储时间等
         * $ticket    null    传 0 或 null
         */
        $this->channel->queue_declare('send_sms', false, true, false, false);
        $data = time() . 'hi， 我是王美丽';
        // 4、发送消息
        $msg = new AMQPMessage($data, ['content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        $this->channel->basic_publish($msg, '', 'send_sms');
        // 5、关闭通道
        $this->channel->close();
        // 关闭连接
        $this->mqConnection->close();
    }

    // 工作模式-系统自动确认：生产者
    public function workerMp()
    {
        $this->channel->queue_declare('worker_task', false, true, false, false);
        for ($i = 0; $i < 10; $i++) {
            $data = $i . ' hi， 我是王美丽 ' . mt_rand(1000000, 9999999);
            $msg = new AMQPMessage($data, ['content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
            $this->channel->basic_publish($msg, '', 'worker_task');
        }
        $this->channel->close();
        $this->mqConnection->close();
    }

    /**
     * 简单模式与工作模式回顾：
     * 1、生产者（producer）是发布消息的应用程序；
     * 2、队列（queue）用于消息存储的缓冲；
     * 3、消费者（consumer）是接收消息的应用程序。
     */
    /**
     * RabbitMQ 消息模型的核心概念是：生产者不会直接发送消息给任何队列。实际上，生产者不知道消息是否已经投递到队列。
     * 生产者（Producer）只需要消息发送给一个交换机（Exchange）。交换机从生产者那里接收消息，然后把消息推送到队列。交换机相当于是一个中间人，它负责把接收到的消息推送到指定的队列。
     * 交换机有四种类型：直连交换机（direct）, 主题交换机（topic）, （头交换机）headers和 扇型交换机（fanout）。我们使用 fanout 来实现发布订阅。
     */

    // 发布订阅模式：生产者
    public function fanoutMq()
    {
        $exchangeName = 'logs';
        $data = date('Y-m-d H:i:s') . ' 王美丽来了';

        // 1、声明交换机
        $this->channel->exchange_declare($exchangeName, 'fanout', false, false, false);
        $msg = new AMQPMessage($data, ['content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        // 2、把消息推送到交换机
        $this->channel->basic_publish($msg, 'logs');

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 路由模式：生产者
    public function directMq()
    {
        // 日志模式: info,error
        $logMode = 'error';
        $exchangeName = 'my-logs';

        // 1、声明交换机
        $this->channel->exchange_declare($exchangeName, 'direct', false, false, false);
        $msg = new AMQPMessage($logMode, ['content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        // 指定 Routing Key
        $this->channel->basic_publish($msg, $exchangeName, $logMode);

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 主题模式：生产者

    /**
     * 主题交换机
     * 发送到主题交换机的路由键必须是一个以 . 分隔开的词语列表。如users.id、user.age。
     * 绑定键也必须拥有同样的格式。主题交换机背后的逻辑跟直连交换机很相似 —— 一个携带着特定路由键的消息会被主题交换机投递给绑定键与之想匹配的队列。但是它的绑定键和路由键有两个特殊应用方式：
     * (* 星号) 用来表示一个单词.
     * # (井号) 用来表示任意数量（零个或多个）单词。
     */
    public function topicMq()
    {
        // 固定.格式，用户后续消费者匹配使用
        // user.info、user.error、user.warn
        $routingKey = 'user.error';
        $exchangeName = 'topic-logs';
        // 1、声明交换机 主题模式
        $this->channel->exchange_declare($exchangeName, 'topic', false, false, false);
        $msg = new AMQPMessage($routingKey, ['content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        $this->channel->basic_publish($msg, $exchangeName, $routingKey);
        $this->channel->close();
        $this->mqConnection->close();
    }

    /**
     * 死信交换机
     * DLX（Dead Letter Exchange）死信交换机。当一个消息在队列中变成死信后，会被 publish 到死信交换机，然后再配置一个队列监听它，这个队列就是死信队列。
     * 死信队列的配置需要两个参数：x-dead-letter-exchange 和 'x-dead-letter-routing-key。下面开始演示死信队列的案例。
     */
    // 死信队列：生产者
    public function deadMq()
    {
        $data = '死信队列测试-消息被拒绝死信队列';
        $exchangeName = 'my-logs';
        $queueName = 'user-log-1';
        $routingKey = 'user';
        $this->channel->exchange_declare($exchangeName, 'direct', false, false, false);

        /**
         * 消息过期
         * x-message-ttl 与 expiration 有什么不一样？
         * x-message-ttl：队列中已过期的消息在队列头部，RabbitMQ 只要定期从队头开始扫描是否有过期的消息即可。
         * expiration：该消息即将被消费时判断是否过期，过期则删除。
         */
        $args = new AMQPTable([
//            'x-message-ttl' => 10, // 整个队列过期时间
          'expiration' => 10, // 设置单个消息过期时间
          'x-dead-letter-exchange' => 'dead-exc', // 死信交换机
          'x-dead-letter-routing-key' => 'dead-key' // 死信路由键
        ]);

        /**
         * 队列长度达到最大长度
         *
         * 设置队列长度限制
         * x-max-length 设置最大消息数；
         * x-max-length-bytes 设置最大长度（以字节为单位）。
         * 如果设置了两个参数，则两者都将适用，将强制执行首先达到的限制。
         *
         * 队列溢出行为
         * x-overflow 设置溢出行为，可选值为 drop-head（默认）、 reject-publish 或 reject-publish-dlx。
         * drop-head：从队列前端（即队列中最旧的消息）删除或死信消息。
         * reject-publish：直接丢弃最近发布的消息。假设 x-max-length = 5，发送消息 1-10，最终剩消息 1-5。
         * reject-publish-dlx：最近发布的消息会进入死信队列。假设 x-max-length = 5，发送消息 1-10，最终消息 1-5 进入队列，消息 6-10 会进入死信队列。
         */
//        $args = new AMQPTable([
//          'x-max-length' => 5, // 设置最大消息数
//          'x-overflow' => 'reject-publish-dlx', // 队列溢出行为
//          'x-dead-letter-exchange' => 'dead-exc', // 死信交换机
//          'x-dead-letter-routing-key' => 'dead-key'  // 死信路由键
//        ]);

        // 通过队列额外参数设置过期时期等配置
        $this->channel->queue_declare($queueName, false, true, false, false, false, $args);
        $this->channel->queue_bind($queueName, $exchangeName, $routingKey);

        /**
         * 死信队列的配置
         */
        $deadExchangeName = 'dead-exc';
        $deadQueueName = 'dead-log-queue';
        $deadRoutingKey = 'dead-key';
        // 1、声明死信交换机
        $this->channel->exchange_declare($deadExchangeName, 'direct', false, false, false);
        // 2、声明死信队列
        $this->channel->queue_declare($deadQueueName, false, true, false, false);
        // 3、死信队列与死信交换机绑定
        $this->channel->queue_bind($deadQueueName, $deadExchangeName, $deadRoutingKey);

        // 正常队列发送消息
        $msg = new AMQPMessage($data, ['content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        $this->channel->basic_publish($msg, $exchangeName, $routingKey);

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 生产者超次尝试次数进入死信队列
    public function deadMqRetry()
    {
        $exchangeName = 'exchange-task';
        $queueName = 'worker-task';
        $routingKey = 'worker_task_user';

        // 创建交换机
        $this->channel->exchange_declare($exchangeName, 'direct', false, false, false);

        // 创建队列
        $this->channel->queue_declare($queueName, false, true, false, false);

        // 将队列名与交换器名进行绑定，并指定routing_key
        $this->channel->queue_bind($queueName, $exchangeName, $routingKey);

        // 头部额外参数-重试次数设置
        $headers = new AMQPTable([
          'retry' => 0
        ]);
        $data = ' hi， 我是王美丽 ' . mt_rand(1000000, 9999999);
        // 正常队列发送消息
        $msg = new AMQPMessage($data, ['application_headers' => $headers, 'content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        $this->channel->basic_publish($msg, $exchangeName, $routingKey);

        $this->channel->close();
        $this->mqConnection->close();
    }
}


$p = new retryP();
// 简单模式：生产者
//$p->simpleMq();

// 工作模式-系统自动确认：生产者
//$p->workerMp();

// 发布订阅模式：生产者
//$p->fanoutMq();

// 路由模式：生产者
//$p->directMq();

// 主题模式：生产者
//$p->topicMq();

// 死信队列：生产者
//$p->deadMq();

// 死信队列：生产者超次尝试次数进入死信队列
$p->deadMqRetry();
