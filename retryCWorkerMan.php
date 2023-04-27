<?php
/**
 * workerman 常驻内存模式消费
 */
require_once('vendor/autoload.php');

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Workerman\Worker;
use Workerman\Lib\Timer;
use Workerman\Connection\TcpConnection;
use Workerman\Connection\AsyncUdpConnection;
use Workerman\Connection\AsyncTcpConnection;

class retryCWorkerMan
{

    public function retryCDade()
    {
        // Linux 下使用创建一个Worker监听23485端口，使用http协议通讯
//        $worker = new Worker('http://0.0.0.0:2348');
        $worker = new Worker();
        //开启进程数量 Linux 下开启才有效
//        $worker->count = 10;
//        $worker->name = "rabbitmq_comsumer";

        $worker->onWorkerStart = function () {
//            global $rabbit_connection, $rabbit_channel, $rabbitmq_exchange_name, $rabbitmq_queueName;
            global $db;
            $db = new \Workerman\MySQL\Connection('127.0.0.1', '3306', 'root', '123456', 'webman_admin');

            $rabbitmq_exchange_name = "workerman-exchange";
            $rabbitmq_queueName = "workerman-queue";
            $rabbitmq_routing_key = "workerman_user";

            // 连接 rabbitmq 服务
            $rabbit_connection = new AMQPStreamConnection('127.0.0.1', '5672', 'guest', 'guest');

            // 获取信道
            $rabbit_channel = $rabbit_connection->channel();

            // 声明队列
            $rabbit_channel->queue_declare($rabbitmq_queueName, false, true, false, false);

            // 绑定队列
            $rabbit_channel->queue_bind($rabbitmq_queueName, $rabbitmq_exchange_name, $rabbitmq_routing_key);

            // 消费者订阅队列
            $rabbit_channel->basic_consume($rabbitmq_queueName, '', false, false, false, false,
              function (AMQPMessage $msg) use ($db) {
                  $exchange = $msg->getExchange();
                  $routingKey = $msg->getRoutingKey();
                  $channel = $msg->getChannel();
                  $body = $msg->getBody();
                  // echo "{$body}\n";
                  $data = json_decode($body, true);
                  // headersObject 是一个AMQPTable对象
                  $headersObject = $msg->get_properties()['application_headers'];
                  // 调用getNativeData()得到一个数组
                  $headersArray = $headersObject->getNativeData();

                  // 具体看 mysql Connection.php 基类
//                  $row = $db->select('id,name,content')->from('wa_test')->where('name like :name')->bindValues(array('name' => '李%'))->row();
//                  echo json_encode($row, JSON_UNESCAPED_UNICODE) . PHP_EOL;
//                  $res = $db->delete('wa_test')->where('id = :id')->bindValues(array('id' => $row['id']))->query();
//                  echo $res . PHP_EOL;

                  try {
                      // 这里是业务处理逻辑
                      // 如果这条消息处理失败，你可以在这里将其再次放回消息队列（最好给消息做个放回去的次数判断，避免无限失败和无限循环）
                      // 注意！！！ insert 和 update 数组后面不要多 , 逗号会导致自增id错乱，错误写法如：array('name'=>'kkkk',)
                      // 查询
                      $row = $db->select('id,name,content')->from('wa_test')->where('name= :name')->bindValues(array('name' => $data['name']))->row();
                      if ($row) {
                          $res = $db->update('wa_test')->cols(array('content' => $row['content'], 'update_time' => date('Y-m-d H:i:s')))->where('id= :id')->bindValues(array('id' => $row['id']))->query();
                      } else {
                          $res = $db->insert('wa_test')->cols(array(
                            'name' => $data['name'],
                            'content' => $data['time']
                          ))->query();
                      }
                      if ($res) {
                          echo "{$data['name']}\n";
                          // 消息确认，表明已经收到这条信息
                          $msg->ack();
                      } else {
                          throw new \Exception('超3次确认失败');
                      }
                  } catch (\Exception $e) {
                      //TODO 超过三次,自己实现业务逻辑
                      echo $e->getMessage() . PHP_EOL;
                      // 就告警 入库db操作,或入队列
                      if ($headersArray['retry'] < 3) {
                          $headersArray['retry']++;//次数+1
                          echo '第' . $headersArray['retry'] . '次失败,消息重新入队' . PHP_EOL;
                          $channel->basic_publish(
                            new AMQPMessage($body, ['application_headers' => new AMQPTable($headersArray), 'content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
                            $exchange,
                            $routingKey
                          );

                          // 回复server,表示处理成功.
                          // 实际上消费者失败了,但是我们把消息重新发送给队列
                          // 所以这里可以认为处理成功
                          $msg->ack();
                      } else {
                          $exchangeName = 'workerman-exchange-dead';
                          $queueName = 'workerman-queue-dead';
                          $routingKey = 'workerman_user_dead';

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
                          $dead_msg = new AMQPMessage($body, ['application_headers' => $headers, 'content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
                          $channel->basic_publish($dead_msg, $exchangeName, $routingKey);

                          $msg->ack();
                      }
                  }
              });

            //这里是重点，网上很多教程起的使用while死循环容易导致程序异步代码无法执行，这种方法能避免
            //按照每个进程每秒处理1000条来设定定时器，每个进程每秒消费1000条，4个进程每秒消费4000条，经过实际验证，将时间改小也无法提升单个进程的处理速度
            //实际测试，4个进程每秒的消费能力有4000左右，可以满足很多中小型系统的应用，如果想提升系统处理能力，
            //可以增加消费者进程数量来解决，比如我将进程数量提升到10个，每秒处理能力约为1万
            //这个机制，希望能力更强的你来进行优化 0.001 毫秒
            Timer::add(1, function () use ($rabbit_channel) {
                if ($rabbit_channel->is_open()) {
                    $rabbit_channel->wait();
                }
            });
        };

        Worker::runAll();
    }
}

$c = new retryCWorkerMan();
$c->retryCDade();