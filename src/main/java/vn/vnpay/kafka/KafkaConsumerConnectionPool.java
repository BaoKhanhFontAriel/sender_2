package vn.vnpay.kafka;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.vnpay.util.ExecutorSingleton;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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
    private static AtomicReference<String> res = new AtomicReference<>(); ;
    private static CountDownLatch latch;

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
            instancePool.consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            instancePool.consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            instancePool.consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }
        return instancePool;
    }

    public synchronized LinkedBlockingQueue<KafkaConsumerConnectionCell> getPool() {
        return pool;
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

    public static void startPoolPolling() {
        log.info("Start Kafka consumer pool polling.........");
        for (KafkaConsumerConnectionCell consumerCell : instancePool.pool) {
            log.info("consumer {} start polling", consumerCell.getConsumer().groupMetadata().groupInstanceId());
            ExecutorSingleton.submit((Runnable) () ->
            {
                while (true) {
                    ConsumerRecords<String, String> records = consumerCell.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> r : records) {
                        log.info("----");
                        log.info("kafka consumer id {} receive data: partition = {}, offset = {}, key = {}, value = {}",
                                consumerCell.getConsumer().groupMetadata().groupInstanceId(),
                                r.partition(),
                                r.offset(), r.key(), r.value());
                        res.set(r.value());
                        latch.countDown();
                        log.info("Thread {} get res {}" , Thread.currentThread().getName(), res.get());

                    }
                }//
            });
        }
    }

    public static synchronized String getRecord() throws TimeoutException, InterruptedException {
        log.info("Get Kafka Consumer pool record.......");
        latch = new CountDownLatch(1);

        try {
            boolean isFinish = latch.await(2000, TimeUnit.MILLISECONDS);
            if (!isFinish){
                throw new TimeoutException("Kafka can not consumes record due to time out");
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            log.error("Kafka is interrupted");
            throw new InterruptedException("Kafka is interrupted");
        }
        log.info("{} latch count after await {}" , Thread.currentThread().getName(), latch.getCount());
        return res.get();
    }
}
