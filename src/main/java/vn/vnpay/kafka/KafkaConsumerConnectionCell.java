package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


@Slf4j
@Getter
@Setter
public class KafkaConsumerConnectionCell {
    private long relaxTime;
    private long timeOut;
    private boolean isClosed;
    private KafkaConsumer<String, String> consumer;
    public KafkaConsumerConnectionCell(Properties consumerProps, String consumerTopic, long timeOut) {
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Arrays.asList(consumerTopic));
        log.info("consumer {} is subscribe to topic {}", consumer.groupMetadata().groupId(), consumerTopic);
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
