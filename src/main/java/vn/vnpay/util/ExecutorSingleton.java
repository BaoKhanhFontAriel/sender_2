package vn.vnpay.util;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


@Slf4j
@Getter
@Setter
@ToString
public class ExecutorSingleton {
    private static ExecutorSingleton instance;
    private  ScheduledExecutorService executorService;

    public ExecutorSingleton(){
        log.info("create new ExecutorServiceSingleton...");
        this.executorService = Executors.newScheduledThreadPool(10);
    }

    public static ExecutorSingleton getInstance(){
        if(instance == null){
            instance = new ExecutorSingleton();
        }
        return instance;
    }

    public static void shutdownNow() {
        instance.executorService.shutdownNow();
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }
    public static void shutdown(){
    }
    public static void wakeup(){
        instance.executorService = Executors.newScheduledThreadPool(10);
    }
}
