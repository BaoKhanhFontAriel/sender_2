package vn.vnpay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.vnpay.controller.ApiController;
import vn.vnpay.kafka.KafkaConfig;
import vn.vnpay.kafka.KafkaProducerPool;
import vn.vnpay.service.ApiService;
import vn.vnpay.thread.ShutdownThread;
import vn.vnpay.util.AppConfigSingleton;
import vn.vnpay.util.ExecutorSingleton;
import vn.vnpay.util.KafkaUtils;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

//@Slf4j
@ApplicationPath("/rest")
public class MainApp extends Application {
    private  final static Logger log = LoggerFactory.getLogger(MainApp.class);
    private Set<Object> singleton = new HashSet<>();
    private Set<Class<?>> classes = new HashSet<>();

    private AtomicReference<LinkedList<String>> responses;
    public MainApp() {
        log.info("start Main......");

        singleton.add(new ApiController());
        classes.add(ApiService.class);

//        AppConfigSingleton.getInstance().readonfig();
//        KafkaUtils.createNewTopic(KafkaPoolConfig.KAFKA_PRODUCER_TOPIC, 10, (short) 1);
        ExecutorSingleton.getInstance();
        KafkaConfig.getInstance().setKafkaServer(AppConfigSingleton.getInstance().getStringProperty("kafka.server"));
        KafkaConfig.getInstance().setKafkaConnectionTimeout(30000);
        KafkaConfig.getInstance().setKafkaProducerTopic("khanh-payment-topic");
        KafkaProducerPool.getInstancePool();
//        KafkaConsumerPool.getInstancePool();
        KafkaUtils.startPoolPolling();
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
    }

    @Override
    public Set<Object> getSingletons() {
        return singleton;
    }

    @Override
    public Set<Class<?>> getClasses() {
        return classes;
    }
}
