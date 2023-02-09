package vn.vnpay.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import vn.vnpay.error.ErrorCode;
import vn.vnpay.kafka.*;
import vn.vnpay.models.ApiResponse;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class KafkaUtils {
    static KafkaConsumerConnectionPool consumerPool = KafkaConsumerConnectionPool.getInstancePool();
    private static volatile String res = null;
    private static volatile boolean isStopping = false;
    private static CountDownLatch latch;

    public static String sendAndReceive(String message) {
        send(message);
        String res = receive();
        log.info("response is: {}", res);
        return res;
    }

    public static String receive() {
        log.info("Kafka receive.........");
        res = null;
        latch = new CountDownLatch(1);
        for (KafkaConsumerConnectionCell consumerCell : consumerPool.getPool()) {
            ExecutorSingleton.submit((Runnable) () ->
            {
                while (true) {
                    ConsumerRecords<String, String> records = consumerCell.getConsumer().poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> r : records) {
                        log.info("----");
                        log.info("kafka consumer id {} receive data: partition = {}, offset = {}, key = {}, value = {}",
                                consumerCell.getConsumer().groupMetadata().groupInstanceId(),
                                r.partition(),
                                r.offset(), r.key(), r.value());
                        res = r.value();
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await(KafkaConnectionPoolConfig.TIME_OUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.info("Kafka consumer can not poll result", e);
            res = GsonSingleton.toJson(new ApiResponse(ErrorCode.MULTI_THREAD_ERROR, e.getMessage(), null));
        }

        ExecutorSingleton.shutdownNow();
        ExecutorSingleton.wakeup();
        return res;
    }

    public static void send(String message) {
        log.info("Kafka send.........");
        log.info("get kafka pool size: {}", KafkaProducerConnectionPool.getInstancePool().getPool().size());
        KafkaProducerConnectionCell producerCell = KafkaProducerConnectionPool.getInstancePool().getConnection();
        KafkaProducer<String, String> producer = producerCell.getProducer();
        // send message
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConnectionPoolConfig.KAFKA_PRODUCER_TOPIC, message);
        producer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                log.info("Kafka producer successfully send record as: Topic = {}, partition = {}, Offset = {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            } else {
                log.error("Can't produce,getting error", e);
            }
        });

        KafkaProducerConnectionPool.getInstancePool().releaseConnection(producerCell);
    }

    public static void createNewTopic(String topic, int partition, short replica) {
        //        //create partition
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        AdminClient adminClient = AdminClient.create(props);

        NewTopic newTopic = new NewTopic(topic, partition, replica);
        adminClient.createTopics(Arrays.asList(newTopic));

//        Map<String, NewPartitions> newPartitionSet = new HashMap<>();
//        newPartitionSet.put(KafkaConnectionPoolConfig.KAFKA_PRODUCER_TOPIC, NewPartitions.increaseTo(partition));
//        adminClient.createPartitions(newPartitionSet);

        adminClient.close();
    }
}
