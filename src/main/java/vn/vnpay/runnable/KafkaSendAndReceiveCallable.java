package vn.vnpay.runnable;

import lombok.extern.slf4j.Slf4j;
import vn.vnpay.models.ApiRequest;
import vn.vnpay.util.GsonSingleton;
import vn.vnpay.util.KafkaUtils;

import java.util.concurrent.Callable;


@Slf4j
public class KafkaSendAndReceiveCallable implements Callable<String> {
    private ApiRequest apiRequest;
    public KafkaSendAndReceiveCallable(ApiRequest apiRequest) {
        this.apiRequest = apiRequest;
    }

    @Override
    public String call() throws Exception {
        String data = GsonSingleton.toJson(apiRequest);
        String answer = KafkaUtils.sendAndReceive(data);
        log.info("answer = {}", answer);
        return answer;
    }
}
