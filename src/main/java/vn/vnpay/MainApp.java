package vn.vnpay;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import vn.vnpay.controller.ApiController;
import vn.vnpay.kafka.KafkaConsumerConnectionPool;
import vn.vnpay.kafka.KafkaProducerConnectionPool;
import vn.vnpay.thread.ShutdownThread;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@ApplicationPath("/rest")
public class MainApp extends Application {
    private Set<Object> singleton = new HashSet<>();
    private Set<Class<?>> classes = new HashSet<>();

    public MainApp() throws JoranException, IOException {
        log.info("start Main......");

        singleton.add(new ApiController());

        initLoggerConfig();
        KafkaConsumerConnectionPool.getInstancePool().start();
        KafkaProducerConnectionPool.getInstancePool().start();
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

    private static void initLoggerConfig() throws IOException, JoranException {
        log.info("start loading logger config......");
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();
        JoranConfigurator configurator = new JoranConfigurator();
        String location = new File(".").getCanonicalPath() + "/config/logback.xml";
        configurator.setContext(loggerContext);
        configurator.doConfigure(location);
    }
}
