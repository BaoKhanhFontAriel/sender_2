package vn.vnpay.thread;


import lombok.extern.slf4j.Slf4j;
import vn.vnpay.kafka.*;
import vn.vnpay.rabbit.RabbitConnectionPool;

@Slf4j
public class ShutdownThread extends Thread{
    RabbitConnectionPool rabbitConnectionPool = RabbitConnectionPool.getInstancePool();
    private static final KafkaConnectionPool kafkaConnectionPool = KafkaConnectionPool.getInstancePool();
    private static final KafkaConsumerConnectionPool kafkaConsumerConnectionPool = KafkaConsumerConnectionPool.getInstancePool();
    private static final KafkaProducerConnectionPool kafkaProducerConnectionPool = KafkaProducerConnectionPool.getInstancePool();
    public void run() {
//        kafkaConnectionPool.getPool().forEach(KafkaConnectionCell::close);
//        rabbitConnectionPool.getPool().forEach(RabbitConnectionCell::close);
        kafkaProducerConnectionPool.getPool().forEach(KafkaProducerConnectionCell::close);
        kafkaConsumerConnectionPool.getPool().forEach(KafkaConsumerConnectionCell::close);
        rabbitConnectionPool.getPool().clear();
        kafkaConnectionPool.getPool().clear();
        kafkaProducerConnectionPool.getPool().clear();
        kafkaConsumerConnectionPool.getPool().clear();
        System.out.println("shut down hook task completed..");
    }
}
