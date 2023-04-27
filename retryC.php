<?php
/**
 * demo 参考 https://ziruchu.com/art/530
 */

require_once('vendor/autoload.php');

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class retryC
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

    // 简单模式：消息消费者
    public function loadSimpleMq()
    {
        // 3、创建队列 持久化设置：设置第三个参数为 true
        $this->channel->queue_declare('send_sms', false, true, false, false);

        $callback = function ($msg) {
            // 输出消费的消息
            // 后续逻辑处理
            echo $msg->body . PHP_EOL;
        };

        // 4、消费者使用消息
        /**
         * $queue  队列名称
         * $consumer_tag     消费者标签。用于区分多个消费者
         * $no_local    false    AMQP 标准，但 RabbitMQ 没有实现
         * $no_ack    false    收到消息后，是否不需要回复确认即被认为被消费。为 true 时，表示自动应答；为 false 时，表示手动应答
         * $exclusive    false    设置是否排他。排他消费者，即这个队列只能由一个消费者消费，适用于任务不允许进行并发处理的情况
         * $nowait    false    为 true 时，表示不等待服务器回执信息，函数将返回 NULL，若开启了排序，则必须等待结果
         * $callback    null    回调函数
         * $arguments    array    额外配置参数
         */
        $this->channel->basic_consume('send_sms', '', false, true, false, false, $callback);

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        // 5、关闭连接
        $this->channel->close();
        $this->mqConnection->close();
    }

    // 工作模式-系统自动确认：消息消费者
    public function loadWorkerMp()
    {
        // 3、创建队列 持久化设置：设置第三个参数为 true
        $this->channel->queue_declare('worker_task', false, true, false, false);

        $callback = function ($msg) {
            sleep(1);
            // 输出消费的消息
            echo $msg->body . PHP_EOL;
        };

        # 消费者使用消息，自动确认回复 no_ack为true
        $this->channel->basic_consume('worker_task', '', false, true, false, false, $callback);

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 工作模式-系统手动确认：消息消费者
    public function loadWorkerMpAck()
    {
        // 3、创建队列 持久化设置：设置第三个参数为 true
        $this->channel->queue_declare('worker_task', false, true, false, false);

        $callback = function ($msg) {
            sleep(1);
            // 输出消费的消息
            echo $msg->body . PHP_EOL;
            // 第三步：手动确认消息
            $msg->ack();
        };

        // 第一步：修改为手动确认
        $this->channel->basic_consume('worker_task', '', false, false, false, false, $callback);
        // 第二步：收到确认后再处理下一条，公平调度：告诉 RabbitMQ，同一时刻，不要发送超过 1 条消息给一个 worker，知道它已经处理完上一条消息并做了 ack 确认。
        $this->channel->basic_qos(null, 1, null);

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 发布订阅模式：消息消费者
    public function loadFanoutMqAck()
    {
        // 1、声明交换机
        $this->channel->exchange_declare('logs', 'fanout', false, false, false);
        // 2、声明队列（随机）获取队列名称并绑定交换机
        list($queueName, ,) = $this->channel->queue_declare("", false, false, true, false);
        // 3、队列绑定交换机
        $this->channel->queue_bind($queueName, 'logs');

        $callback = function ($msg) {
            $body = $msg->body;
            echo $body . PHP_EOL;
            $msg->ack();
        };
        // 公平调度
        $this->channel->basic_qos(null, 1, null);
        // 消息消费
        $this->channel->basic_consume($queueName, '', false, false, false, false, $callback);

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 路由模式：消息消费者
    public function loadDirectMq()
    {
        // 日志模式：info、error
        $logMode = 'error';
        $exchangeName = 'my-logs';

        // 1、声明交换机
        $this->channel->exchange_declare($exchangeName, 'direct', false, false, false);
        // 2、声明队列（随机）获取队列名称并绑定交换机
        list($queueName, ,) = $this->channel->queue_declare("", false, false, true, false);
        // 3、队列绑定交换机
        $this->channel->queue_bind($queueName, $exchangeName, $logMode);

        $callback = function ($msg) {
            echo '输出： ' . $msg->body . PHP_EOL;
            $msg->ack();
        };

        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($queueName, '', false, false, false, false, $callback);

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 主题模式：消息消费者
    public function loadTopicMq()
    {
        /**
         * 1、使用 # 匹配，# 会接收所有日志
         * 2、使用 *.error ，只匹配 后缀为 error 的消息
         * 3、使用 devices.* ，只接收 devices 开头的队列消息
         */
        // 接收参数参数用于匹配
        $logMode = '#';

        // 1、声明交换机
        $this->channel->exchange_declare('topic-logs', 'topic', false, false, false);

        list($queueName, ,) = $this->channel->queue_declare("", false, false, true, false);
        $this->channel->queue_bind($queueName, 'topic-logs', $logMode);

        $callback = function ($msg) {
            echo '输出： ' . $msg->body . PHP_EOL;
            $msg->ack();
        };

        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($queueName, '', false, false, false, false, $callback);

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 死信队列:：消息消费者、正常业务
    public function loadDeadMq()
    {
        $exchangeName = 'my-logs';
        $routingKey = 'user';
        $this->channel->exchange_declare($exchangeName, 'direct', false, false, false);

        list($queueName, ,) = $this->channel->queue_declare("", false, false, true, false);

        $this->channel->queue_bind($queueName, $exchangeName, $routingKey);

        // 正常消费者
        $callback = function (AMQPMessage $msg) {
            echo '输出： ' . $msg->body . PHP_EOL;
            $msg->ack();
        };

        // 拒绝接收消息，异常消息者
//        $callback = function (AMQPMessage $msg) {
//            // 拒绝接收消息
//            $msg->ack(true);
//        };

        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($queueName, '', false, false, false, false, $callback);

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 死信队列:：异常消息消费者
    public function loadDeadMqErr()
    {
        $deadExchangeName = 'dead-exc';
        $deadQueueName = 'dead-log-queue';
        $deadRoutingKey = 'dead-key';
        $this->channel->exchange_declare($deadExchangeName, 'direct', false, false, false);

        $this->channel->queue_bind($deadQueueName, $deadExchangeName, $deadRoutingKey);

        $callback = function ($msg) {
            echo '从死信队列中输出的消息： ' . $msg->body . PHP_EOL;
            $msg->ack();
        };

        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($deadQueueName, '', false, false, false, false, $callback);
        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->mqConnection->close();
    }

    // 死信队列:：消息消费者超过重试次数进入死信
    public function loadDeadMqRetry()
    {
        $exchangeName = 'exchange-task';
        $queueName = 'worker-task';
        $routingKey = 'worker_task_user';

        // 3、创建队列 持久化设置：设置第三个参数为 true
        $this->channel->queue_declare($queueName, false, true, false, false);

        // 关键点是借助getNativeData()方法获取信息,判断重试次数.
        // 重试次数+1重新发送消息到原队列
        // 拒绝接收消息，异常消息者
        $callback = function (AMQPMessage $msg) {
            try {
                //业务逻辑
                throw new \Exception('消费失败');
            } catch (\Exception $e) {
                $exchange = $msg->getExchange();
                $routingKey = $msg->getRoutingKey();
                $channel = $msg->getChannel();
                $body = $msg->getBody();
                //headersObject 是一个AMQPTable对象
                $headersObject = $msg->get_properties()['application_headers'];
                //调用getNativeData()得到一个数组
                $headersArray = $headersObject->getNativeData();
                if ($headersArray['retry'] < 3) {
                    $headersArray['retry']++;//次数+1
                    echo '第' . $headersArray['retry'] . '次失败,消息重新入队' . PHP_EOL;
                    $channel->basic_publish(
                      new AMQPMessage($body, ['application_headers' => new AMQPTable($headersArray), 'content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
                      $exchange,
                      $routingKey
                    );

                    //回复server,表示处理成功.
                    //实际上消费者失败了,但是我们把消息重新发送给队列
                    //所以这里可以认为处理成功
                    $msg->ack();
                } else {
                    //TODO 超过三次,自己实现业务逻辑
                    echo '失败次数过多,直接丢弃,可以自己决定如何处理' . PHP_EOL;
                    //就告警 入库db操作,或入队列

                    $exchangeName = 'exchange-task-dead';
                    $queueName = 'worker-task-dead';
                    $routingKey = 'worker_task_user_dead';

                    // 创建交换机
                    $channel->exchange_declare($exchangeName, 'direct', false, false, false);

                    // 创建队列
                    $channel->queue_declare($queueName, false, true, false, false);

                    // 将队列名与交换器名进行绑定，并指定routing_key
                    $channel->queue_bind($queueName, $exchangeName, $routingKey);

                    // 头部额外参数-重试次数设置
                    $headers = new AMQPTable([
                      'dead' => 1
                    ]);
                    // 尝试超3次
                    $dead_msg = new AMQPMessage($body.'我被抛弃了 3 次', ['application_headers' => $headers, 'content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
                    $channel->basic_publish($dead_msg, $exchangeName, $routingKey);

                    $msg->ack();
                }
            }
        };

        $this->channel->basic_consume($queueName, '', false, false, false, false, $callback);

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->mqConnection->close();
    }
}

$c = new retryC();
// 简单模式：消息消费者
//$c->loadSimpleMq();

// 工作模式-系统自动确认：消息消费者
//$c->loadWorkerMp();

// 工作模式-系统手动确认：消息消费者
//$c->loadWorkerMpAck();

// 发布订阅模式：消息消费者
//$c->loadFanoutMqAck();

// 路由模式：消息消费者
//$c->loadDirectMq();

// 主题模式：消息消费者
//$c->loadTopicMq();

// 1. 先执行生产者生产消息，再执行正常消费者产生异常消息，2. 执行异常费者消费异常消息
// 1. 死信队列:：消息消费者、正常业务
//$c->loadDeadMq();
// 2. 死信队列: 异常消息消费者
//$c->loadDeadMqErr();


// 死信队列:：消息消费者超过重试次数进入死信
for ($i = 0; $i < 5; $i++) {
    $c->loadDeadMqRetry();
}

