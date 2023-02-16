package vn.vnpay.kafka;
import vn.vnpay.util.AppConfigSingleton;

public class KafkaPoolConfig {

    public static final int MAX_CONSUMER_POOL_SIZE = AppConfigSingleton.getInstance().getIntProperty("kafka.max_consumer_pool_size");
    public static final long TIME_OUT = AppConfigSingleton.getInstance().getIntProperty("kafka.timeout");;
    public static final String KAFKA_CONSUMER_TOPIC = AppConfigSingleton.getInstance().getStringProperty("kafka.topic.consumer");
    public static final String KAFKA_CONSUMER_GROUP_ID = AppConfigSingleton.getInstance().getStringProperty("kafka.consumer.group_id");
    public static final String KAFKA_PRODUCER_TOPIC = AppConfigSingleton.getInstance().getStringProperty("kafka.topic.producer");
    public static final String KAFKA_SERVER = AppConfigSingleton.getInstance().getStringProperty("kafka.server");
}
