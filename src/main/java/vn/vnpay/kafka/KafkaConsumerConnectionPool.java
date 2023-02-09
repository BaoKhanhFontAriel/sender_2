package vn.vnpay.kafka;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class KafkaConsumerConnectionPool {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConnectionPool.class);

    private LinkedBlockingQueue<KafkaConsumerConnectionCell> pool = new LinkedBlockingQueue<>();

    private static KafkaConsumerConnectionPool instancePool;

    protected int numOfConnectionCreated = 0;
    protected int maxPoolSize;
    protected int initPoolSize;
    protected int minPoolSize;
    protected long timeOut = 10000;
    protected String url;
    protected Properties consumerProps;
    protected String consumerTopic;
    protected Thread thread;
    protected long startTime;
    protected long endTime;
    public synchronized static KafkaConsumerConnectionPool getInstancePool() {
        if (instancePool == null) {
            instancePool = new KafkaConsumerConnectionPool();
            instancePool.initPoolSize = KafkaConnectionPoolConfig.INIT_CONSUMER_POOL_SIZE;
            instancePool.consumerTopic = KafkaConnectionPoolConfig.KAFKA_CONSUMER_TOPIC;
            String bootstrapServers = KafkaConnectionPoolConfig.KAFKA_SERVER;
            String grp_id = KafkaConnectionPoolConfig.KAFKA_CONSUMER_GROUP_ID;
            instancePool.consumerProps = new Properties();
            instancePool.consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            instancePool.consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
//            instancePool.consumerProps.setProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, ConsumerConfig.De);
            instancePool.consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            instancePool.consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            instancePool.consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }
        return instancePool;
    }

    public void start() {
        log.info("Create Kafka Consumer Connection pool........................ ");
        // Load Connection to Pool
        startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < initPoolSize; i++) {
                KafkaConsumerConnectionCell connection = new KafkaConsumerConnectionCell(consumerProps, consumerTopic, i);
                pool.put(connection);
                numOfConnectionCreated++;
            }
        } catch (Exception e) {
            log.warn("[Message : can not start connection pool] - [Connection pool : {}] - " + "[Exception : {}]",
                    this.toString(), e);
        }

        endTime = System.currentTimeMillis();
        log.info("Start Kafka Consumer Connection pool in : {} ms", (endTime - startTime));
    }
}
