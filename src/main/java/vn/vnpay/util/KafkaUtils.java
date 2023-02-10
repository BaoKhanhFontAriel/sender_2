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
import vn.vnpay.models.ApiRequest;
import vn.vnpay.models.ApiResponse;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class KafkaUtils {
    static KafkaConsumerConnectionPool consumerPool = KafkaConsumerConnectionPool.getInstancePool();
//    private static volatile String res = null;

    //    private static AtomicReference<String> res = new AtomicReference<>("");
//    private static AtomicReference<LinkedList<String>> res = new AtomicReference<>(new LinkedList<>());
    private static volatile boolean isStopping = false;

    public static String sendAndReceive(ApiRequest apiRequest) {
        log.info("send and receive: {}", apiRequest);
        send(apiRequest);
        String res = receive(apiRequest);
        log.info("response is: {}", res);
        return res;
    }


    public static String receive(ApiRequest apiRequest) {
        log.info("Kafka receive.........");
        String initValue = GsonSingleton.toJson(new ApiResponse(ErrorCode.KAFKA_ERROR, "empty value", apiRequest.getToken()));
        AtomicReference<String> res = new AtomicReference<>(initValue);
        Set<Future<String>> futureSet = new HashSet<>();
        final  CountDownLatch latch = new CountDownLatch(1);

            log.info("consumer pool size: {}", consumerPool.getPool().size());
        for (KafkaConsumerConnectionCell consumerCell : consumerPool.getPool()) {
            Future future = ExecutorSingleton.submit((Runnable) () ->
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
                    }
                }//

            });
            futureSet.add(future);
        }

        try {
            latch.await(60000, TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {
            log.info("Kafka consumer can not poll result", e);
            res.set( GsonSingleton.toJson(new ApiResponse(ErrorCode.MULTI_THREAD_ERROR, e.getMessage(), null)));
//            res = GsonSingleton.toJson(new ApiResponse(ErrorCode.MULTI_THREAD_ERROR, e.getMessage(), null));
        }

        return res.get();
    }

    public static void send(ApiRequest apiRequest) {
        log.info("Kafka send.........");
        KafkaProducerConnectionCell producerCell = KafkaProducerConnectionPool.getInstancePool().getConnection();
        KafkaProducer<String, String> producer = producerCell.getProducer();
        // send message

        String message = GsonSingleton.toJson(apiRequest);
        log.info("message send {}", message);
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
