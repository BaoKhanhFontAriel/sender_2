package vn.vnpay.kafka.runnable;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import vn.vnpay.kafka.*;

import java.time.Duration;
import java.util.concurrent.Callable;


@Slf4j
public class KafkaSendAndReceiveCallable implements Callable<String> {
    private String message;

    public KafkaSendAndReceiveCallable(String message) {
        this.message = message;
    }

    private String sendAndReceive() {
        String response = null;
        KafkaConsumerConnectionCell consumerCell = KafkaConsumerConnectionPool.getInstancePool().getConnection();
        KafkaProducerConnectionCell producerCell = KafkaProducerConnectionPool.getInstancePool().getConnection();
        KafkaConsumer<String, String> consumer = consumerCell.getConsumer();
        KafkaProducer<String, String> producer = producerCell.getProducer();

        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConnectionPoolConfig.KAFKA_PRODUCER_TOPIC, message);
        producer.send(record, (recordMetadata, e) -> {
            if (e == null) {
//                log.info("Kafka producer successfully send record as: Topic = {}, partition = {}, Offset = {}",
//                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            } else {
                log.error("Can't produce,getting error", e);
            }
        });

        // polling
        log.info("Kafka consumer {} waiting for message...", consumer.groupMetadata());
        try {
            while (true) {
                synchronized (this){

                }
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> r : records) {
                        log.info("----");
//                        log.info("rabbit begin receiving data: offset = {}, key = {}, value = {}",
//                                r.offset(), r.key(), r.value());
                        response = (String) r.value();
                        return response;
                    }
            }
        } catch (Exception e) {
            log.error("Unsuccessfully poll ", e);
        } finally {
            KafkaProducerConnectionPool.getInstancePool().releaseConnection(producerCell);
            KafkaConsumerConnectionPool.getInstancePool().releaseConnection(consumerCell);
        }
        return response;
    }

//    @Override
//    public String call() throws Exception {
//        return sendAndReceive();
//    }

//    @Override
//    public void run() {
//        sendAndReceive();
//    }

    @Override
    public String call() throws Exception {
        String answer = sendAndReceive();
        return answer;
    }
}
