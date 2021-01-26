<?php

namespace app\command;

use think\console\Command;
use think\console\Input;
use think\console\Output;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Consumer;
use RdKafka\TopicConf;

class KakfaConsumer extends Command
{
    protected function configure()
    {
        // 指令配置
        $this->setName('kafka:consume');
        // 设置参数
        
    }


    public function execute(Input $input, Output $output)
    {
        //$this->lowLevel($input, $output);

        $this->highLevel($input, $output);
    }



    /** 低级消费 则需要自己配置消费的topic和特定的分区 */
    public function lowLevel(Input $input, Output $output)
    {
        $group_id = 'IOT_MANAGE_ID_2';
        $topic = 'foo';
        $host = 'kafka:9092';

        $conf = new Conf();

        $conf->set('log_level', (string) LOG_INFO);
        $conf->set('debug', 'consumer');
        $conf->set('group.id', $group_id);
        $conf->set('client.id', 'low_level_client_new');
        $conf->set('metadata.broker.list', $host);

        $conf->set('socket.timeout.ms', 50); // or socket.blocking.max.ms, depending on librdkafka version
        $conf->set('fetch.wait.max.ms', 49);
        if (function_exists('pcntl_sigprocmask')) {
            pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
            $conf->set('internal.termination.signal', SIGIO);
        } else {
            $conf->set('queue.buffering.max.ms', 1);
        }

        // 高版本
        $conf->set('enable.auto.commit', 'false'); // 高版本是否设置自动提交
        $conf->set('enable.auto.offset.store', 'false'); // 当需要手动提交offset的时候 需要设置该参数

        $topicConfig = new TopicConf();
        // 设置生产消息是否需要回复确认
        // 0不需要确认 1 则是需要leader副本来确认 follow副本不需要 -1 需要等到leader副本和follow副本都确认(此时回调会带有offset)
        //$conf->set('request.required.acks', -1); // 生产者进行配置


        // 设置消息消费确认
        //$topicConfig->set('auto.commit.enable', 0); // 是否开启自动提交确认消费 0 不开启 1 开启
        $topicConfig->set('auto.commit.interval.ms', 1000); // 如果不开启自动提交消费offset 这项则没必要配置

        // 设置broker的存储介质
        $topicConfig->set('offset.store.method', 'broker'); // 高版本只有broker一个选项 file已经被废弃
        $topicConfig->set('auto.offset.reset', 'smallest'); // 这个配置就是当offset没有初始化的时候 或者访问到一个不存在的offset的时候 该怎么重置

        $rk = new Consumer($conf);
        $rk->addBrokers($host);

        // 消费的topic
        $topic = $rk->newTopic($topic, $topicConfig);
        // 设置从哪里开始消费
        $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED); // 从最后一次的offset开始消费

        while (true) {
            $message = $topic->consume(0, 3000);
            if (is_null($message)) {
                sleep(1);
                echo 'No more message'.PHP_EOL;
                continue;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    echo 'receive new message from broker'.PHP_EOL;
                    var_dump($message);
                    // 确认消息
                    //$topic->offsetStore($message->partition, $message->offset);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo 'No more messages; will wait for more'.PHP_EOL;
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo 'Timed out'.PHP_EOL;
                    break;
                default:
                    throw new \RuntimeException($message->errstr(), $message->err);
            }
        }
    }


     /** 高级消费 最大的特点就是方便 动态的分配topic分区 */
    protected function highLevel(Input $input, Output $output)
    {
        $group_id = 'IOT_MANAGE_ID_2';
        $topic = 'foo';
        $host = 'kafka:9092';

        $conf = new Conf();
        // 当有新的消费者进程加入或者退出的时候 kafka会自动重新分配给消费者进程 这里注册了一个回调函数 当分区被重新分配时触发
        $conf->setRebalanceCb(function (KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo 'Assign: ';
                    var_dump($partitions);
                    $kafka->assign($partitions);
                    break;
                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    echo 'Revoke: ';
                    var_dump($partitions);
                    $kafka->assign(null);
                    break;
                default:
                    throw new \RuntimeException($err);
            }
        });


        // 配置group.id 具有相同的group.id的consumer将会处理不同分区的消息
        $conf->set('group.id',$group_id);
        $conf->set('client.id', 'consumer_client_'.mt_rand(1,999));
        $conf->set('metadata.broker.list', $host);

        // 针对低延迟进行了优化的配置 允许php进程尽快发送消息并快速 停止
        $conf->set('socket.timeout.ms', 50);

        if (function_exists('pcntl_sigprocmask')) {
            pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
            $conf->set('internal.termination.signal', SIGIO);
        } else {
            $conf->set('queue.buffering.max.ms', 1);
        }



        // 在interval.ms的时间内自动提交确认
        // 高版本
        $conf->set('enable.auto.commit', 'false'); // 高版本是否设置自动提交
        //$conf->set('enable.auto.offset.store', 'false'); // 当需要手动提交offset的时候 需要设置该参数
        $conf->set('auto.commit.interval.ms', 100);
        // 提交offset到broker的时间间隔 跟是否开启自动提交无关 手动添加[只能添加到librdkafka队列中 还是需要定时提交] 也许定时提交

        // 自动重置offset
        $conf->set('auto.offset.reset', 'smallest'); // 这个参数主要用于当不存在store_offset 或者 一个越界的offset 该怎么处理offset 
        //smallest 则是从当前topic特定的分区的最开始端开始消费
        
        // 保存偏移量 防止重复消费
        // 设置offset的存储为broker
        // 设置offset的存储为broker 需要配置group.id 否则会出现问题
        //$conf->set('offset.store.method', 'broker'); // 高版本中 保存offset只能保存在broker中了 设置与否都无所谓了

        $consumer = new KafkaConsumer($conf);

        // 订阅主题  自动分配partitions
        $consumer->subscribe([$topic]);

        //        指定topic分配partitions使用那个分区
//        $consumer->assign([
//            new \RdKafka\TopicPartition("zzy8", 0),
//            new \RdKafka\TopicPartition("zzy8", 1),
//            ]);
        while (true) {
            $message = $consumer->consume(3000);
            if ($message) {
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        var_dump('New message received :', $message);
                        var_dump($message->payload);
                        var_dump($message->key);
                        $consumer->commit($message); //提交offset
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        echo 'no more message; will wait for more'.PHP_EOL;
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        echo 'time out'.PHP_EOL;
                        var_dump('#################');
                        break;
                    default:
                        var_dump('noting');
                        throw new \RuntimeException($message->errstr(), $message->err);
                }
            } else {
                var_dump('this is empty message');
            }
        }
    }
}
