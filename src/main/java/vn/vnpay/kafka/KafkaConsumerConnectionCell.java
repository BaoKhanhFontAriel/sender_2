package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
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
        consumerProps.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, member_id);

//        consumer.
        this.consumer = new KafkaConsumer<>(consumerProps);
//        TopicPartition partition = new TopicPartition(consumerTopic, index);

        this.consumer.subscribe(Arrays.asList(consumerTopic));
        this.consumer.poll(Duration.ofMillis(100));

        log.info("consumer {} - member {} is assign to topic {} - partition {}",
                consumer.groupMetadata().groupId(), consumer.groupMetadata().memberId(), consumerTopic, consumer.assignment());
    }

    public void subscribeTopic(String... consumerTopic) {
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
            consumer.unsubscribe();
            consumer.close();
            isClosed = true;
        } catch (Exception e) {
            log.warn("connection is closed: {0}", e);
        }
    }
}
