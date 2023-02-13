package vn.vnpay.redis;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import vn.vnpay.models.ApiResponse;
import vn.vnpay.util.ExecutorSingleton;
import vn.vnpay.util.GsonSingleton;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


@Slf4j
@Getter
@Setter
@ToString
public class RedisConnectionPool {
    private LinkedBlockingQueue<RedisConnectionCell> pool = new LinkedBlockingQueue<>();
    protected int numOfConnectionCreated = 0;

    protected int maxPoolSize;
    protected int initPoolSize;
    protected int minPoolSize;
    protected long timeOut = 10000;

    protected String url;

    protected Thread thread;
    protected long startTime;
    protected long endTime;
    protected static RedisConnectionPool instancePool;

    public synchronized static RedisConnectionPool getInstancePool() {
        if (instancePool == null) {
            instancePool = new RedisConnectionPool();
            instancePool.initPoolSize = RedisConnectionPoolConfig.INIT_POOL_SIZE;
            instancePool.maxPoolSize = RedisConnectionPoolConfig.MAX_POOL_SIZE;
            instancePool.minPoolSize = RedisConnectionPoolConfig.MIN_POOL_SIZE;
            instancePool.url = RedisConnectionPoolConfig.URL;
            instancePool.timeOut = RedisConnectionPoolConfig.TIME_OUT;
            instancePool.thread = new Thread(() -> {
                while (true) {
                    for (RedisConnectionCell connection : instancePool.pool) {
                        if (instancePool.numOfConnectionCreated > instancePool.minPoolSize) {
                            if (connection.isTimeOut()) {
                                try {
                                    connection.close();
                                    instancePool.pool.remove(connection);
                                    instancePool.numOfConnectionCreated--;
                                } catch (Exception e) {
                                    log.warn("Waring : Connection can not close in timeOut !");
                                }
                            }
                        }
                    }
                }
            });

        }
        return instancePool;
    }

    public void start() {
        log.info("Create Reddis Connection pool........................ ");
        // Load Connection to Pool
        startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < initPoolSize; i++) {
                RedisConnectionCell connection = new RedisConnectionCell(url, timeOut);
                pool.put(connection);
                numOfConnectionCreated++;
            }
        } catch (Exception e) {
            log.warn(String.format(
                    "[Message : can not start oracle connection pool] - [Connection pool : %s] - " + "[Exception : %s]",
                    this.toString(), e));
        }
        thread.start();
        endTime = System.currentTimeMillis();
        log.info("Start Rabbit Connection pool in : {} ms", (endTime - startTime));
    }

    public synchronized RedisConnectionCell getConnection() {
        log.info("Get Redis connection....");
        RedisConnectionCell connectionWraper = null;
        if (pool.size() == 0 && numOfConnectionCreated < maxPoolSize) {
            connectionWraper = new RedisConnectionCell(url, timeOut);
            try {
                pool.put(connectionWraper);
            } catch (InterruptedException e) {
                log.warn("Can not PUT Connection to Pool, Current Poll size = " + pool.size()
                        + " , Number Connection : " + numOfConnectionCreated, e);
                e.printStackTrace();
            }
            numOfConnectionCreated++;
        }
        try {
            connectionWraper = pool.take();
        } catch (InterruptedException e) {
            log.warn("Can not GET Connection from Pool, Current Poll size = " + pool.size()
                    + " , Number Connection : " + numOfConnectionCreated);
            e.printStackTrace();
        }
        connectionWraper.setRelaxTime(System.currentTimeMillis());
        return connectionWraper;
    }


    public void releaseConnection(RedisConnectionCell conn) {
        log.info("Redis release connection.........");
        try {
            if (conn.isClosed()) {
                pool.remove(conn);
                RedisConnectionCell connection = new RedisConnectionCell(url, timeOut);
                pool.put(connection);
            } else {
                pool.put(conn);
            }
        } catch (Exception e) {
            log.error("Connection : " + conn.toString(), e);
        }
    }

    public static void setExpire(String key, String value, int expireTime) {
        CountDownLatch latch = new CountDownLatch(1);
        Future future = ExecutorSingleton.getInstance().getExecutorService().submit(() -> {
            RedisConnectionCell redisCell = RedisConnectionPool.getInstancePool().getConnection();
            redisCell.getJedis().set(key, value);
            redisCell.getJedis().expire(key, expireTime);
            RedisConnectionPool.getInstancePool().releaseConnection(redisCell);
            latch.countDown();
        });

        try {
            latch.await(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        future.cancel(true);
    }
}
