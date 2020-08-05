<?php
/**
 * Created by PhpStorm.
 * User: tianjiong
 * Date: 2018/6/1
 * Time: 下午8:58
 */
class Smallapp_Kafka_KafkaConsumer
{
    public $_consumer;
    public function __construct($topic) {
        $broker = 'kafka.bj.bce-internal.baidu.com:9071';
        $securityProtocol = 'ssl';
        $clientPem = __DIR__ . '/' . 'client.pem';
        $clientKey = __DIR__ . '/' . 'client.key';
        $caPem = __DIR__ . '/' . 'ca.pem';
        $groupId = 'kafka-samples-php-consumer-'.explode('__', $topic)[0];
        $conf = new RdKafka\Conf();
        $conf->set('metadata.broker.list', $broker);
        $conf->set('group.id', $groupId);
        $conf->set('security.protocol', $securityProtocol);
        $conf->set('ssl.certificate.location', $clientPem);
        $conf->set('ssl.key.location', $clientKey);
        $conf->set('ssl.ca.location', $caPem);
        $conf->set('receive.message.max.bytes', 5*1024*1024);
        $this->_consumer = new RdKafka\KafkaConsumer($conf);
        $this->_consumer->subscribe([$topic]);
    }
    public function consumer($msgLen) {
        try{
            for ($i = 0; $i < $msgLen; $i++) {
                $message = $this->_consumer->consume(120*1000);
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        var_dump("partition:" . $message->partition . ", offset:" . $message->offset . ", " . $message->payload);
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        var_dump("No more messages; will wait for more");
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        var_dump("Timed out");
                        break;
                    default:
                        throw new Exception($message->errstr(), $message->err);
                }
            }
        } catch (Exception $e) {
            Bd_Log::warning("produce meg to kafka fail!!! msg is " . $e->getMessage());
            return false;
        }
        return true;
    }
}
