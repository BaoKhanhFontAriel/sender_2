package vn.vnpay.util;

import lombok.extern.slf4j.Slf4j;
import vn.vnpay.kafka.KafkaConsumerPool;
import vn.vnpay.kafka.KafkaProducerPool;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class KafkaUtils {
    private static final AtomicReference<LinkedBlockingQueue<String>> recordQueue = new AtomicReference<>(new LinkedBlockingQueue<>());

    public static String sendAndReceive(String data) throws Exception {
        log.info("send and receive: {}", data);
        send(data);
        String res = receive();
        log.info("response is: {}", res);
        return res;
    }

    public static void send(String message) throws Exception {
        KafkaProducerPool.getInstance().send(message);
    }

    public static String receive() throws Exception {
        log.info("Kafka start receiving.........");
        return KafkaConsumerPool.getInstance().getRecord();
    }

    public static void startPoolPolling() {
        log.info("Start Kafka consumer pool polling.........");
        int count = 10;
        while (count > 0) {
            try {
                KafkaConsumerPool.getInstance().createConsumerPolling();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            count--;
        }
    }

}
