package vn.vnpay;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.vnpay.controller.ApiController;
import vn.vnpay.kafka.KafkaConnectionPoolConfig;
import vn.vnpay.kafka.KafkaConsumerConnectionPool;
import vn.vnpay.kafka.KafkaProducerConnectionPool;
import vn.vnpay.service.ApiService;
import vn.vnpay.thread.ShutdownThread;
import vn.vnpay.util.KafkaUtils;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

//@Slf4j
@ApplicationPath("/rest")
public class MainApp extends Application {
    private  final static Logger log = LoggerFactory.getLogger(MainApp.class);
    private Set<Object> singleton = new HashSet<>();
    private Set<Class<?>> classes = new HashSet<>();

    private AtomicReference<LinkedList<String>> responses;
    public MainApp() throws JoranException, IOException {
        log.info("start Main......");

        singleton.add(new ApiController());
        classes.add(ApiService.class);

        KafkaUtils.createNewTopic(KafkaConnectionPoolConfig.KAFKA_CONSUMER_TOPIC, 10, (short) 1);

        responses = new AtomicReference<>();
        responses.set(new LinkedList<>());
        log.info("atomic responses {}:", responses);

        KafkaConsumerConnectionPool.getInstancePool().start();
        KafkaProducerConnectionPool.getInstancePool().start();
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        log.info("finish Main startup");
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
