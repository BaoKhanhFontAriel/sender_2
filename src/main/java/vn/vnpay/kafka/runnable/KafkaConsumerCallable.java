package vn.vnpay.kafka.runnable;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import vn.vnpay.kafka.KafkaConsumerConnectionCell;
import vn.vnpay.kafka.KafkaConsumerConnectionPool;

import java.time.Duration;
import java.util.concurrent.Callable;


@Slf4j
public class KafkaConsumerCallable implements Callable<String> {
    private volatile String response;

    public void shutdown(){

    }

    @Override
    public String call() throws Exception {
        response = null;
        KafkaConsumerConnectionCell consumerCell = KafkaConsumerConnectionPool.getInstancePool().getConnection();
        KafkaConsumer<String, String> consumer = consumerCell.getConsumer();

        // polling
        log.info("Get Kafka consumer {} - partition {}", consumer.groupMetadata().groupId(), consumer.assignment());
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> r : records) {
                    log.info("----");
//                    log.info("rabbit begin receiving data: offset = {}, key = {}, value = {}",
//                            r.offset(), r.key(), r.value());
                    return (String) r.value();
                }
            }
        } catch (Exception e) {
            log.error("Unsuccessfully poll ", e);
        }
        return response;
    }
}
