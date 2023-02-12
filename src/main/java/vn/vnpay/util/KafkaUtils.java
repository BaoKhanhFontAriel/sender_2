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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class KafkaUtils {
    public static String sendAndReceive(String message) throws Exception {
        log.info("send and receive: {}", message);
        send(message);
        String res = receive();
        log.info("response is: {}", res);
        return res;
    }

    public static String receive() throws TimeoutException, InterruptedException {
        log.info("Kafka start receiving.........");
        return KafkaConsumerConnectionPool.getRecord();
    }

    public static void send(String message) throws Exception {
        log.info("Kafka send.........");
        KafkaProducerConnectionCell producerCell = KafkaProducerConnectionPool.getInstancePool().getConnection();
        KafkaProducer<String, String> producer = producerCell.getProducer();

        // send message
        log.info("message send {}", message);
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConnectionPoolConfig.KAFKA_PRODUCER_TOPIC, message);
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


        KafkaProducerConnectionPool.getInstancePool().releaseConnection(producerCell);
    }

    public static void createNewTopic(String topic, int partition, short replica) {
        //        //create partition
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        AdminClient adminClient = AdminClient.create(props);

        NewTopic newTopic = new NewTopic(topic, partition, replica);
        adminClient.createTopics(Arrays.asList(newTopic));

        adminClient.close();
    }
}
