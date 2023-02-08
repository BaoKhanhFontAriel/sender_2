package vn.vnpay.kafka.runnable;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import vn.vnpay.kafka.KafkaConsumerConnectionCell;

import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;


@Slf4j
public class KafkaConsumerRunner implements Runnable{
    private AtomicReference<LinkedList<String>> responses;
    private KafkaConsumerConnectionCell consumerCell;
    public KafkaConsumerRunner(KafkaConsumerConnectionCell consumer) {
        this.consumerCell = consumer;
    }
    @Override
    public void run() {
        log.info("atomic reponses {}", responses);
        KafkaConsumer<String, String> consumer = consumerCell.getConsumer();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> r : records) {
                    log.info("----");
                    log.info("kafka consumer {} receive data: offset = {}, key = {}, value = {}",
                            consumer.groupMetadata().groupId(),
                            r.offset(), r.key(), r.value());
//                    response = r.value();
//                    response
                        responses.get().add(r.value());
                        log.info("Atomic responses add {} to last", responses.get().peekLast());
                    return;
                }
            }
        } catch (Exception e) {
            log.error("Unsuccessfully poll ", e);
        }
    }
}
