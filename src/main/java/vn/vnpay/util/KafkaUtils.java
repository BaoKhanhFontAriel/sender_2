package vn.vnpay.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import vn.vnpay.kafka.*;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class KafkaUtils {
    public static String sendAndReceive(String message) {
        log.info("start Kafka send and receive.........");
        String response = null;

        // get kafka connection cell
        KafkaConsumerConnectionCell consumerCell = KafkaConsumerConnectionPool.getInstancePool().getConnection();
        KafkaProducerConnectionCell producerCell = KafkaProducerConnectionPool.getInstancePool().getConnection();

        KafkaConsumer<String, String> consumer = consumerCell.getConsumer();
        KafkaProducer<String, String> producer = producerCell.getProducer();


        // send message
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConnectionPoolConfig.KAFKA_PRODUCER_TOPIC, message);
        producer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                log.info("Kafka producer successfully send record as: Topic = {}, partition = {}, Offset = {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            } else {
                log.error("Can't produce,getting error", e);
            }
        });

        // receive message
//        log.info("Kafka consumer {} polling for message...", consumer.groupMetadata());
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> r : records) {
                    log.info("----");
                    log.info("kafka consumer {} receive data: offset = {}, key = {}, value = {}",
                            consumer.groupMetadata().groupId(),
                            r.offset(), r.key(), r.value());
                    response =  r.value();
                    return response;
                }
            }
        } catch (Exception e) {
            log.error("Unsuccessfully poll ", e);
        }
        finally {
            KafkaProducerConnectionPool.getInstancePool().releaseConnection(producerCell);
            KafkaConsumerConnectionPool.getInstancePool().releaseConnection(consumerCell);
        }
        return "response";
    }
}
