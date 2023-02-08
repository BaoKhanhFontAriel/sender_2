package vn.vnpay.kafka.runnable;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import vn.vnpay.kafka.KafkaConsumerConnectionCell;
import vn.vnpay.kafka.KafkaConsumerConnectionPool;
import vn.vnpay.kafka.KafkaProducerConnectionPool;

import java.time.Duration;
import java.util.concurrent.Callable;


//@Slf4j
//public class KafkaConsumerCallable implements Callable<String> {
//    @Override
//    public String call() throws Exception {
//        KafkaConsumerConnectionCell consumerCell = KafkaConsumerConnectionPool.getInstancePool().getConnection();
//        KafkaConsumer<String, String> consumer = consumerCell.getConsumer();
//
//        String response = null;
//        try {
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//                for (ConsumerRecord<String, String> r : records) {
//                    log.info("----");
//                    log.info("kafka consumer {} receive data: offset = {}, key = {}, value = {}",
//                            consumer.groupMetadata().groupId(),
//                            r.offset(), r.key(), r.value());
//                    response = r.value();
//                    return response;
//                }
//            }
//        } catch (Exception e) {
//            log.error("Unsuccessfully poll ", e);
//        } finally {
//            KafkaConsumerConnectionPool.getInstancePool().releaseConnection(consumerCell);
//        }
//        return null;
//    }
//}
