package vn.vnpay.service;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.vnpay.error.ErrorCode;
import vn.vnpay.models.ApiRequest;
import vn.vnpay.models.ApiResponse;
import vn.vnpay.models.PaymentRequest;
import vn.vnpay.runnable.KafkaSendAndReceiveCallable;
import vn.vnpay.util.*;

import java.util.concurrent.*;


public class ApiService {
    private static final Logger log = LoggerFactory.getLogger(ApiService.class);

//    OracleConnectionPool oracleConnectionPool = OracleConnectionPool.getInstancePool();
//    RabbitConnectionPool rabbitConnectionPool = RabbitConnectionPool.getInstancePool();
//    KafkaConnectionPool kafkaConnectionPool = KafkaConnectionPool.getInstancePool();

    private Gson gson = GsonSingleton.getInstance().getGson();
    public String sendToCore(String data)  {
        ApiRequest apiRequest = RequestUtils.createRequest(data);
        Future future = ExecutorSingleton.submit(new KafkaSendAndReceiveCallable(apiRequest));

        String response = GsonSingleton.toJson(new ApiResponse(ErrorCode.KAFKA_ERROR, "", apiRequest.getToken()));
        try {
            response = (String) future.get(60000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            log.info("Kafka can not get response", e);
            response = GsonSingleton.toJson(new ApiResponse(ErrorCode.EXECUTION_ERROR, e.getMessage(), apiRequest.getToken()));
        }
        catch (InterruptedException e) {
            log.info("Thread is interrupted", e);
            response = GsonSingleton.toJson(new ApiResponse(ErrorCode.INTERRUPTED_ERROR, e.getMessage(), apiRequest.getToken()));
        } catch (TimeoutException e) {
            log.info("kafka send and receive is out of time ", e);
            response = GsonSingleton.toJson(
                    new ApiResponse(ErrorCode.MULTI_THREAD_ERROR, "Request is time out", apiRequest.getToken()));
        }

        return response;
    }

    public String sendPayment()  {
        String requestid = TokenUtils.generateNewToken();
        int processTime = MathUtils.getRandomInt(300,1000);
        PaymentRequest paymentRequest =
                PaymentRequest.builder()
                        .requestid(requestid)
                        .processtime(processTime)
                        .build();
        try {
            KafkaUtils.send(paymentRequest.toString());
        } catch (Exception e) {
            return "fail to send to kafka" + paymentRequest.getRequestid();
        }
        return "success send to kafka " + paymentRequest.getRequestid();
    }

//    public String getReceive(){
//        ApiResponse apiResponse = new ApiResponse("00", "success", TokenUtils.generateNewToken());
//        String requestid = TokenUtils.generateNewToken();
//        int processTime = MathUtils.getRandomInt(300,1000);
//        PaymentRequest paymentRequest =
//                PaymentRequest.builder()
//                        .requestid(requestid)
//                        .processtime(processTime)
//                        .build();
//        try {
//            KafkaUtils.send(paymentRequest.toString());
//        } catch (Exception e) {
//            return "fail to send to kafka" + paymentRequest.getRequestid();
//        }
//        return "success send to kafka " + paymentRequest.getRequestid();
//    }
}
