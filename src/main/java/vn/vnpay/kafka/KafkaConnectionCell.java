package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
@Setter
@Getter
public class KafkaConnectionCell {
    private long relaxTime;
    private long timeOut;
    private boolean isClosed;
    private KafkaConsumer<String, String> consumer;
    private String producerTopic;
    private String consumerTopic;
    private KafkaProducer<String, String> producer;

    public KafkaConnectionCell(Properties consumerProps, Properties producerProps,  String consumerTopic, String producerTopic, long timeOut) {
        producer = new KafkaProducer<>(producerProps);
        consumer = new KafkaConsumer<>(consumerProps);

        this.producerTopic = producerTopic;
        this.consumerTopic = consumerTopic;
        this.timeOut = timeOut;
        consumer.subscribe(Arrays.asList(consumerTopic));
        log.info("Subscribed to topic " + consumerTopic);
    }

    public  String sendAndReceive(String message) {

        ProducerRecord<String, String> record = new ProducerRecord<>(producerTopic, message);
        producer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                log.info("Kafka producer successfully send record as: Topic = {}, partition = {}, Offset = {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            }

            else {
                log.error("Can't produce,getting error",e);
            }
        });

        // polling
        log.info("Kafka consumer waiting for message...");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> r : records) {
                log.info("----");
//                log.info("rabbit begin receiving data: offset = {}, key = {}, value = {}",
//                        r.offset(), r.key(), r.value());
                String response = (String) r.value();
                return response;
            }
        }
    }

    public boolean isTimeOut() {
        if (System.currentTimeMillis() - this.relaxTime > this.timeOut) {
            return true;
        }
        return false;
    }

    public void close() {
        try {
            consumer.close();
            isClosed = true;
        } catch (Exception e) {
            log.warn("connection is closed: {0}", e);
        }
    }
}
