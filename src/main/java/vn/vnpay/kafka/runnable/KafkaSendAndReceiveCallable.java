package vn.vnpay.kafka.runnable;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import vn.vnpay.kafka.*;
import vn.vnpay.util.KafkaUtils;

import javax.ws.rs.core.Response;
import java.time.Duration;
import java.util.concurrent.Callable;


@Slf4j
public class KafkaSendAndReceiveCallable implements Callable<String> {
    private String message;

    public KafkaSendAndReceiveCallable(String message) {
        this.message = message;
    }

    @Override
    public String call() throws Exception {
        String answer = KafkaUtils.sendAndReceive(message);
        return answer;
    }
}
