<?php
/**
 * Created by PhpStorm.
 * User: tianjiong
 * Date: 2018/6/1
 * Time: 下午8:58
 */
class Smallapp_Kafka_KafkaProducer
{
    public $_topic;
    public $_rk;
    public function __construct($topic) {
        $broker = 'kafka.bj.bce-internal.baidu.com:9071';
        $securityProtocol = 'ssl';
        $clientPem = __DIR__ . '/' . 'client.pem';
        $clientKey = __DIR__ . '/' . 'client.key';
        $caPem = __DIR__ . '/' . 'ca.pem';
        $conf = new RdKafka\Conf();
        $conf->set('metadata.broker.list', $broker);
        $conf->set('security.protocol', $securityProtocol);
        $conf->set('ssl.certificate.location', $clientPem);
        $conf->set('ssl.key.location', $clientKey);
        $conf->set('ssl.ca.location', $caPem);
        $conf->set('message.max.bytes', 5*1024*1024);
        $this->_rk = new RdKafka\Producer($conf);
        $this->_rk->setLogLevel(LOG_DEBUG);
        $this->_rk->addBrokers($broker);
        $this->_topic = $this->_rk->newTopic($topic);
    }
    public function produceMsg($msg) {
        try{
            $this->_topic->produce(RD_KAFKA_PARTITION_UA, 0, $msg);
            //$this->_rk->poll(1);
        } catch (RdKafka\Exception $e) {
            Bd_Log::warning("produce meg to kafka fail!!! msg is " . $e->getMessage());
            return false;
        }
        //$this->_rk->flush();
        return true;
    }
}
