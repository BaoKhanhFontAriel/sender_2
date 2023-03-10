package vn.vnpay.thread;


import lombok.extern.slf4j.Slf4j;
import vn.vnpay.kafka.KafkaConsumerPool;
import vn.vnpay.kafka.KafkaProducerPool;
import vn.vnpay.rabbit.RabbitConnectionPool;
import vn.vnpay.util.ExecutorSingleton;

@Slf4j
public class ShutdownThread extends Thread{
    RabbitConnectionPool rabbitConnectionPool = RabbitConnectionPool.getInstancePool();
    public void run() {
//        kafkaConnectionPool.getPool().forEach(KafkaConnectionCell::close);
//        rabbitConnectionPool.getPool().forEach(RabbitConnectionCell::close);
        ExecutorSingleton.shutdownNow();
        KafkaConsumerPool.getInstance().shutdown();
        KafkaProducerPool.getInstance().shutdown();
//        rabbitConnectionPool.getPool().clear();
        System.out.println("shut down hook task completed..");
    }
}
