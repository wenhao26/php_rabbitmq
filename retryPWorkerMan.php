<?php
/**
 * workerman 命令 php retryPWorkerMan.php start -d 守护进程，php 指服务器php的版本，如果有多个看运维怎么配置，如 php74 retryPWorkerMan.php start -d 等
 * 重启 php retryPWorkerMan.php restart -d
 * 平滑重启 php retryPWorkerMan.php reload -d
 * 停止 php retryPWorkerMan.php stop
 * 状态 php retryPWorkerMan.php status
 */
require_once('vendor/autoload.php');

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Workerman\Worker;

class retryPWorkerMan
{
    public function retryPDade()
    {
        // Linux 下使用端口号
//        $worker = new Worker("http://0.0.0.0:2347");
        $worker = new Worker();
        //开启进程数量 Linux 下开启才有效
//        $worker->count = 4;
//        $worker->name = "rabbitmq_push";

        $worker->onWorkerStart = function () {
//            global $rabbit_connection, $rabbit_channel, $rabbitmq_exchange_name, $rabbitmq_queueName;
            $rabbitmq_exchange_name = "workerman-exchange";
            $rabbitmq_queueName = "workerman-queue";
            $rabbitmq_routing_key = "workerman_user";

            // 连接 rabbitmq 服务
            $rabbit_connection = new AMQPStreamConnection('127.0.0.1', '5672', 'guest', 'guest');

            // 获取信道
            $rabbit_channel = $rabbit_connection->channel();

            //声明创建交换机
            $rabbit_channel->exchange_declare($rabbitmq_exchange_name, 'topic', false, true, false);

            // 声明创建队列
            $rabbit_channel->queue_declare($rabbitmq_queueName, false, true, false, false);

            // 绑定队列
            $rabbit_channel->queue_bind($rabbitmq_queueName, $rabbitmq_exchange_name, $rabbitmq_routing_key);

            //可以修改时间间隔，如果为0.002秒，则每秒产生500*4=2000条  0.001 毫秒
//            Timer::add(0.002, function () use ($rabbit_channel, $rabbitmq_exchange_name, $rabbitmq_queueName) {
            //需要向rabbitmq队列投递消息的内容，通常为数组，经过json转换再发送
            for ($i = 0; $i < 3; $i++) {

                $data_all = array(
                  'name' => "张三" . $i,
                  'time' => time() . mt_rand(100000, 999999)
                );
                $data_all_out_json = json_encode($data_all, JSON_UNESCAPED_UNICODE);
                // 头部额外参数-重试次数设置
                $headers = new AMQPTable([
                  'retry' => 0
                ]);
                $data_all_out_msg = new AMQPMessage($data_all_out_json, ['application_headers' => $headers, 'content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);

                //向队列里面写内容
                $rabbit_channel->basic_publish($data_all_out_msg, $rabbitmq_exchange_name, $rabbitmq_queueName);
            }
//            });
        };

        Worker::runAll();
    }
}

$p = new retryPWorkerMan();
$p->retryPDade();