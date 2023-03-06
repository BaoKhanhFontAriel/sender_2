package vn.vnpay.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import vn.vnpay.kafka.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class KafkaUtils {
    private static final AtomicReference<LinkedBlockingQueue<String>> recordQueue = new AtomicReference<>(new LinkedBlockingQueue<>());

    public static String sendAndReceive(String data) throws Exception {
        log.info("send and receive: {}", data);
        send("send-topic", data);

        String res = receive();
        log.info("response is: {}", res);
        return res;
    }

    public static void send(String topic, String message) throws Exception {
        log.info("Kafka send.........");
        KafkaProducerCell producerCell = KafkaProducerPool.getInstancePool().getConnection();
        KafkaProducer<String, String> producer = producerCell.getProducer();

        // send message
        log.info("message send {}", message);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        try{
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    log.info("Kafka producer successfully send record as: Topic = {}, partition = {}, Offset = {}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());

                } else {
                    log.error("Can't produce,getting error", e);
                }
            });
        }
        catch (Exception e){
            throw new Exception("Kafka can not produce message");
        }

        KafkaProducerPool.getInstancePool().releaseConnection(producerCell);
    }

    private static AdminClient adminClient;

    public static void createNewTopic(String topic, int partition, short replica) {
        //        //create partition
        if (adminClient == null){
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:29092");
            adminClient = AdminClient.create(props);
        }

        NewTopic newTopic = new NewTopic(topic, 10, replica);
        adminClient.createTopics(Arrays.asList(newTopic));

        Map<String, NewPartitions> map = new HashMap<>();
        map.put(topic, NewPartitions.increaseTo(partition));
        adminClient.createPartitions(map);

        adminClient.close();
    }


    // For Kafka consumer
    public static String receive() throws Exception {
        log.info("Kafka start receiving.........");
        return getRecord();
    }
    public static String getRecord() throws Exception {
        log.info("Get Kafka Consumer pool record.......");
        return recordQueue.get().take();
    }

    public static void startPoolPolling() {
        log.info("Start Kafka consumer pool polling.........");
        int count = 10;
        while (count > 0) {
            KafkaConsumerCell consumerCell = KafkaConsumerPool.getInstancePool().getConnection();
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

                        recordQueue.get().add(r.value());
                    }


                    try {
                        consumerCell.getConsumer().commitSync();
                    } catch (CommitFailedException e) {
                        log.error("commit failed", e);
                    }
                }//
            });
            count--;
        }
    }

}
