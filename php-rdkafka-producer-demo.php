<?php

namespace app\admin\controller;


use RdKafka\Conf;
use RdKafka\Producer;
use think\Request;
use Enqueue\RdKafka\RdKafkaConnectionFactory;
use RdKafka\TopicConf;
use RdKafka\Message;
use Log;

/** 测试kafka相关api */
class Test extends Base
{
    /**
     * 创建kafka topic
     */
    public function createTopic()
    {
        $conf = new Conf();
        $conf->set('log_level', (string)LOG_INFO);
        //$conf->set('debug','all');
        //$conf->set('metadata.broker.list', 'kafka:9092');


        // 生产消息成功回调 [一旦设置了request.require.acks = -1 则message中会带有offset  可以利用这个做数据的保障检测]
        $conf->setDrMsgCb(function ($kafka, Message $message) {
            Log::notice('Kakfa produce record: '.json_encode([
                    'topic_name' => $message->topic_name,
                    'time'       => date('Y-m-d H:i:s', intval($message->timestamp / 1000)),
                    'partition'  => $message->partition,
                    'payload'    => $message->payload,
                    'key'        => $message->key,
                    'offset'     => $message->offset // 这个在  request.require.acks = 0 or 1 是没有的 需要注意
                ]));
        });
        // 生产消息失败回调
        $conf->setErrorCb(function ($kafka, $err, $reason) {
              file_put_contents(PROJECT_PATH."/err_cb.log", sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason).PHP_EOL, FILE_APPEND);
//            echo 'poll 错误回调'.PHP_EOL;
//            var_dump($kafka, $err, $reason);
//            echo '---------------------------'.PHP_EOL;
        });

        // 加统计回调  目前没发现作用的场景
//        $conf->setStatsCb(function ($kafka, $json, $json_len) {
//            var_dump($kafka);
//            var_dump(json_decode($json, true));
//            var_dump($json_len);
//        });

        // 以下三个参数是 配置librdkafka底层的queue队列的一些参数 第一个是 队列最大的等待时间 
        // 第二个是 队列可以存储的最大的数据量条数 但是这里可能存在一个问题 循环produce 可能单条数据量很大 导致既没有达到最大超时，也没有达到最大的数据条数，但是队列满了 此时会报错 queue full
        // 那么就需要设置第三个参数 设置队列的最大大小(Mb)。
        // 我这里只是测试 所以就是生产的单条数据 一般业务中 应该是循环添加多条消息，需要根据业务量 判断是循环多次来poll 还是 循环一次就poll， 在执行时间和内存占用上均衡。
        // 设置消息发送的延迟时间
        $conf->set('queue.buffering.max.ms', 5); // 默认就是5ms
        // 设置队列可以暂时保存的最大数据数目
        $conf->set('queue.buffering.max.messages', 1000000);
        // 设置队列暂时可以保存的最大的数据量
        $conf->set('queue.buffering.max.kbytes', 2000000); // 2G大小
        // 设置librdkafka发送消息数据统计回调
        //$conf->set('statistics.interval.ms', 10);

        $rk = new Producer($conf);

        $topicConfig = new TopicConf();
        $topicConfig->set('request.required.acks', -1);
        $topicConfig->setPartitioner(RD_KAFKA_MSG_PARTITIONER_RANDOM); // 随机分发消息到随机的分区中 防止在生产数据的时候 可能总是在同一个分区

        $rk->addBrokers('kafka:9092');

        $topic = $rk->newTopic('foo',$topicConfig);

        // 生产者发送数据 produce生产的数据首先放在 一个队列中 这个队列可以配置一个最大等待时间和最大的数据条数和最大的数据量
        // 只有当到了最大的等待时间或者达到了最大的数据条数
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, 'fpm message payload '.mt_rand(1,999), 'fpm key');
        $rk->poll(10); // 等待底层队列的数据发送[能执行这个poll 只可能是到了最大的队列等待时间]

        // 重置底层的发送队列
        $rk->flush(5); // 对于poll已经保证了数据全部发送出去了 但是为了保险起见 还是在flush 队列的时候加一点时间
        return 1;
    }


    
}
