package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

@Slf4j
@Getter
@Setter
public class KafkaProducerConnectionCell {
    private long relaxTime;
    private long timeOut;
    private boolean isClosed;
    private KafkaProducer<String, String> producer;
    private String producerTopic;
    public KafkaProducerConnectionCell(Properties producerConfig, String producerTopic, long timeOut) {
        this.producer = new KafkaProducer<>(producerConfig);
        this.producerTopic = producerTopic;
        this.timeOut = timeOut;
        log.info("create producer {}", producer.metrics());
    }

    public boolean isTimeOut() {
        if (System.currentTimeMillis() - this.relaxTime > this.timeOut) {
            return true;
        }
        return false;
    }

    public void close() {
        try {
            producer.close();
            isClosed = true;
        } catch (Exception e) {
            log.warn("connection is closed: {0}", e);
        }
    }
}
