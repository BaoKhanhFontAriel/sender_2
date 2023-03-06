package vn.vnpay.kafka;

import lombok.Getter;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.vnpay.util.ExecutorSingleton;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

@Getter
public class KafkaConsumerPool extends ObjectPool<KafkaConsumerCell> {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerPool.class);
    private static KafkaConsumerPool instancePool;
    protected Properties consumerProps;
    protected String consumerTopic;
    protected Thread thread;
    protected long startTime;
    protected long endTime;

    public synchronized static KafkaConsumerPool getInstancePool() {
        if (instancePool == null) {
            instancePool = new KafkaConsumerPool();
        }
        return instancePool;
    }

    public KafkaConsumerPool() {
        setExpirationTime(Integer.MAX_VALUE);
        setInitSize(KafkaPoolConfig.INIT_CONSUMER_POOL_SIZE);
        consumerTopic = KafkaPoolConfig.KAFKA_CONSUMER_TOPIC;
        consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaPoolConfig.KAFKA_SERVER);
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaPoolConfig.KAFKA_CONSUMER_GROUP_ID);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }

    public KafkaConsumerCell getConnection() {
        return super.checkOut();
    }

    @Override
    protected KafkaConsumerCell create() {
        return new KafkaConsumerCell(consumerProps, consumerTopic);
    }

    @Override
    public boolean validate(KafkaConsumerCell o) {
        return (!o.isClosed());
    }

    @Override
    public void expire(KafkaConsumerCell o) {
        o.close();
    }
}
