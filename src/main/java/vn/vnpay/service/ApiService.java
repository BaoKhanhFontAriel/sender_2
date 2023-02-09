package vn.vnpay.service;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.vnpay.error.ErrorCode;
import vn.vnpay.kafka.KafkaConnectionPoolConfig;
import vn.vnpay.kafka.KafkaConsumerConnectionPool;
import vn.vnpay.kafka.KafkaProducerConnectionPool;
import vn.vnpay.kafka.runnable.KafkaSendAndReceiveCallable;
import vn.vnpay.models.ApiRequest;
import vn.vnpay.models.ApiResponse;
import vn.vnpay.util.ExecutorSingleton;
import vn.vnpay.util.GsonSingleton;
import vn.vnpay.util.KafkaUtils;
import vn.vnpay.util.TokenUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class ApiService {
    private static final Logger log = LoggerFactory.getLogger(ApiService.class);

//    OracleConnectionPool oracleConnectionPool = OracleConnectionPool.getInstancePool();
//    RabbitConnectionPool rabbitConnectionPool = RabbitConnectionPool.getInstancePool();
//    KafkaConnectionPool kafkaConnectionPool = KafkaConnectionPool.getInstancePool();
    KafkaProducerConnectionPool producerPool = KafkaProducerConnectionPool.getInstancePool();
    KafkaConsumerConnectionPool consumerPool = KafkaConsumerConnectionPool.getInstancePool();
    private Gson gson = GsonSingleton.getInstance().getGson();
    public String sendToCore(String data)  {

        String message = createRequest(data);

//        RabbitConnectionCell conn = rabbitConnectionPool.getConnection();
//        String response = conn.sendAndReceive(message);
//        rabbitConnectionPool.releaseConnection(conn);

        Future future = ExecutorSingleton.submit(new KafkaSendAndReceiveCallable(message));

        String response = null;
        try {
            response = (String) future.get(60000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            log.info("Can not execute KafkaSendAndReceiveCallable", e);
            response = GsonSingleton.toJson(new ApiResponse(ErrorCode.MULTI_THREAD_ERROR, e.getMessage(), null));
        }
        catch (InterruptedException e) {
            log.info("Thread is interrupted", e);
            response = GsonSingleton.toJson(new ApiResponse(ErrorCode.MULTI_THREAD_ERROR, e.getMessage(), null));
        } catch (TimeoutException e) {
            log.info("kafka send and receive is out of time", e);
            response = GsonSingleton.toJson(
                    new ApiResponse(ErrorCode.MULTI_THREAD_ERROR, "Request is time out", null));
        }
        return response;
    }

    public String sendToCore2(String data)  {
        String message = createRequest(data);
        String response = KafkaUtils.sendAndReceive(message);
        return response;
    }

    public String createRequest(String data) {
        String token = TokenUtils.generateNewToken();
        ApiRequest customerRequest = new ApiRequest(token, data);
        log.info("create request {}", customerRequest.toString());
        return gson.toJson(customerRequest);
    }
}
