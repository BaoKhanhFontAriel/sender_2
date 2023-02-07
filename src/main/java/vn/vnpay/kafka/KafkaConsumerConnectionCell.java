package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;


@Slf4j
@Getter
@Setter
public class KafkaConsumerConnectionCell {
    private long relaxTime;
    private long timeOut;
    private boolean isClosed;
    private KafkaConsumer<String, String> consumer;
    public KafkaConsumerConnectionCell(Properties consumerProps, String consumerTopic, long timeOut, int index) {
        this.timeOut = timeOut;

        String member_id = "api-consumer" + "-" + index;
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, member_id);

        this.consumer = new KafkaConsumer<>(consumerProps);

        this.consumer.subscribe(Arrays.asList(consumerTopic));
        log.info("consumer {} is subscribe to topic {}", consumer.groupMetadata().groupId(), consumerTopic);
    }

    public void subscribeTopic(String... consumerTopic){
        this.consumer.subscribe(Arrays.asList(consumerTopic));
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
