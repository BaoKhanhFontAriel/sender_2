package vn.vnpay.kafka.runnable;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import vn.vnpay.kafka.KafkaConnectionPoolConfig;
import vn.vnpay.kafka.KafkaConsumerConnectionPool;
import vn.vnpay.kafka.KafkaProducerConnectionCell;
import vn.vnpay.kafka.KafkaProducerConnectionPool;

@Slf4j
public class KafkaProducerRunner implements Runnable {
    private volatile String response;
    private String message;

    public KafkaProducerRunner(String message) {
        this.message = message;
    }

    @Override
    public void run() {
        KafkaProducerConnectionCell producerCell = KafkaProducerConnectionPool.getInstancePool().getConnection();
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
    }
}
