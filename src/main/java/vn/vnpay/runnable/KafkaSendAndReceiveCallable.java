package vn.vnpay.runnable;

import lombok.extern.slf4j.Slf4j;
import vn.vnpay.util.KafkaUtils;

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
