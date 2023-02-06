package vn.vnpay.rabbit;


import com.rabbitmq.client.ConnectionFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

@Setter
@Getter
@ToString
@Slf4j
public class RabbitConnectionPool {

//    private static final Logger log = LoggerFactory.getLogger(RabbitConnectionCell.class);
    private LinkedBlockingQueue<RabbitConnectionCell> pool = new LinkedBlockingQueue<>();

    private  ConnectionFactory factory;

    protected int numOfConnectionCreated = 0;
    protected int maxPoolSize;
    protected int initPoolSize;
    protected int min_pool_size;
    protected long time_out = 10000;

    protected String host;
    protected String queueName;
    protected String exchangeName;
    protected String exchangeType;
    protected String routingKey;

    protected Thread thread;
    protected long start_time;
    protected long end_time;
    protected static RabbitConnectionPool instancePool;

    public synchronized static RabbitConnectionPool getInstancePool() {
        if (instancePool == null) {
            instancePool = new RabbitConnectionPool();
            instancePool.initPoolSize = RabbitConnectionPoolConfig.INIT_POOL_SIZE;
            instancePool.maxPoolSize = RabbitConnectionPoolConfig.MAX_POOL_SIZE;
            instancePool.min_pool_size = RabbitConnectionPoolConfig.MIN_POOL_SIZE;
            instancePool.factory =  new ConnectionFactory();
            instancePool.factory.setHost(RabbitConnectionPoolConfig.HOST);
            instancePool.queueName = RabbitConnectionPoolConfig.QUEUE_NAME;
            instancePool.exchangeName = RabbitConnectionPoolConfig.EXCHANGE_NAME;
            instancePool.exchangeType = RabbitConnectionPoolConfig.EXCHANGE_TYPE;
            instancePool.routingKey = RabbitConnectionPoolConfig.ROUTING_KEY;
            instancePool.time_out = RabbitConnectionPoolConfig.TIME_OUT;
            /*
             * When the number of connection > min connection , close TimeOut Connection
             */
            instancePool.thread = new Thread(() -> {
                for (RabbitConnectionCell connection : instancePool.pool) {
                    if (instancePool.numOfConnectionCreated > instancePool.min_pool_size) {
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
            });
        }
        return instancePool;
    }

    public void start() {
        log.info("Create Connection pool........................ ");
        // Load Connection to Pool
        start_time = System.currentTimeMillis();
        try {
            for (int i = 0; i < initPoolSize; i++) {
                RabbitConnectionCell connection = new RabbitConnectionCell(factory, exchangeName, exchangeType, routingKey, time_out);
                pool.put(connection);
                numOfConnectionCreated++;
            }
        } catch (Exception e) {
            log.warn("[Message : can not start connection pool] - [Connection pool : {}] - " + "[Exception : {}]",
                    this.toString(), e);
        }

        // declare reply queue
        try {
            RabbitConnectionCell conn = pool.take();
            conn.getChannel().queueDeclare(RabbitConnectionPoolConfig.REPLY_TO, true,false, false, null);
//            releaseConnection(conn);
        } catch (InterruptedException | IOException e) {
            log.info("fail to create reply queue: ", e);
        }

        thread.start();
        end_time = System.currentTimeMillis();
        log.info("Start Connection pool in : {} ms", (end_time - start_time));
    }

    public synchronized RabbitConnectionCell getConnection() {
        log.info("begin creating rabbit connection!");
        RabbitConnectionCell connectionWraper = null;
        if (pool.size() == 0 && numOfConnectionCreated < maxPoolSize) {
            connectionWraper = new RabbitConnectionCell(factory, exchangeName, exchangeType, routingKey, time_out);
            try {
                pool.put(connectionWraper);
            } catch (InterruptedException e) {
                log.warn("Can not PUT Connection to Pool, Current Poll size = " + pool.size()
                        + " , Number Connection : " + numOfConnectionCreated, e);
                e.printStackTrace();
            }
            numOfConnectionCreated++;
        }
        log.info("finish creating rabbit connection");
        try {
            connectionWraper = pool.take();
        } catch (InterruptedException e) {
            log.warn("Can not GET Connection from Pool, Current Poll size = " + pool.size()
                    + " , Number Connection : " + numOfConnectionCreated);
            e.printStackTrace();
        }
        connectionWraper.setRelaxTime(System.currentTimeMillis());
        log.info("set relax time: {}", connectionWraper.getRelaxTime());
        return connectionWraper;
    }


    public void releaseConnection(RabbitConnectionCell conn) {
        log.info("begin releasing connection {}", conn.toString());
        try {
            if (conn.isClosed()) {
                pool.remove(conn);
                RabbitConnectionCell connection = new RabbitConnectionCell(factory, exchangeName, exchangeType, routingKey, time_out);
                pool.put(connection);
            } else {
                pool.put(conn);
            }
            log.info("successfully release connection {}", conn.toString());
        } catch (Exception e) {
            log.info("Connection : " + conn.toString(), e);
        }
    }

}
