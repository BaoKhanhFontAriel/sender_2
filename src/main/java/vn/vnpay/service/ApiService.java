package vn.vnpay.service;

import co.paralleluniverse.fibers.Fiber;
import com.google.gson.Gson;
import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.vnpay.error.ErrorCode;
import vn.vnpay.models.ApiRequest;
import vn.vnpay.models.ApiResponse;
import vn.vnpay.models.PaymentRequest;
import vn.vnpay.runnable.KafkaSendAndReceiveCallable;
import vn.vnpay.runnable.SendPaymentCallable;
import vn.vnpay.util.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class ApiService {
    private static final Logger log = LoggerFactory.getLogger(ApiService.class);

//    OracleConnectionPool oracleConnectionPool = OracleConnectionPool.getInstancePool();
//    RabbitConnectionPool rabbitConnectionPool = RabbitConnectionPool.getInstancePool();
//    KafkaConnectionPool kafkaConnectionPool = KafkaConnectionPool.getInstancePool();

    private Gson gson = GsonSingleton.getInstance().getGson();

    public String sendToCore(String data) {
        ApiRequest apiRequest = RequestUtils.createRequest(data);
        String response = StringUtils.EMPTY;

//        Future future = ExecutorSingleton.submit(new KafkaSendAndReceiveCallable(apiRequest));

        Future future = new Fiber(() -> {
            String convert  = GsonSingleton.toJson(apiRequest);
            String res = null;
            try {
                res = KafkaUtils.sendAndReceive(convert);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return res;
        });
        try {
            response = (String) future.get(60000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            log.info("Kafka can not get response", e);
            response = GsonSingleton.toJson(new ApiResponse(ErrorCode.EXECUTION_ERROR, e.getMessage(), apiRequest.getToken()));
        } catch (InterruptedException e) {
            log.info("Thread is interrupted", e);
            response = GsonSingleton.toJson(new ApiResponse(ErrorCode.INTERRUPTED_ERROR, e.getMessage(), apiRequest.getToken()));
        } catch (TimeoutException e) {
            log.info("kafka send and receive is out of time ", e);
            response = GsonSingleton.toJson(
                    new ApiResponse(ErrorCode.MULTI_THREAD_ERROR, "Request is time out", apiRequest.getToken()));
        }

        return response;
    }

    public String sendToCore2(String data) {
        ApiRequest apiRequest = RequestUtils.createRequest(data);
        String response = null;
        try {
            response = KafkaUtils.sendAndReceive(GsonSingleton.toJson(apiRequest));
        } catch (Exception e) {
            response = GsonSingleton.toJson(
                    new ApiResponse(ErrorCode.KAFKA_ERROR, e.getMessage(), apiRequest.getToken()));
            ;
        }
        log.info("response return = {}", response);

        return response;
    }

    public String sendPayment() {
        String requestid = TokenUtils.generateNewToken();
        int processTime = MathUtils.getRandomInt(300, 1000);
        PaymentRequest paymentRequest =
                PaymentRequest.builder()
                        .requestid(requestid)
                        .processtime(processTime)
                        .build();
        StringBuilder sb = new StringBuilder();
        String response = StringUtils.EMPTY;

        Future future = new Fiber<>(() -> {
            try {
                KafkaUtils.send("khanh-payment-topic", paymentRequest.toString());
            } catch (Exception e) {
                return sb.append("fail to send to kafka ").append(paymentRequest.getRequestid()).toString();
            }
            return sb.append("success to send to kafka ").append(paymentRequest.getRequestid()).toString();
        }).start();

        try {
            response = (String) future.get(60000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            response = sb.append("interrupted request = ").append(paymentRequest.getRequestid()).toString();
        } catch (ExecutionException e) {
            response = sb.append("execution request = ").append(paymentRequest.getRequestid()).toString();
        } catch (TimeoutException e) {
            response = sb.append("time out request = ").append(paymentRequest.getRequestid()).toString();
        }
        return response;
    }
}
