package vn.vnpay.rabbit;

import com.rabbitmq.client.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Setter
@Getter
@ToString
@Slf4j
public class RabbitConnectionCell {

//    private static final Logger log = LoggerFactory.getLogger(RabbitConnectionCell.class);
    private String exchangeName;

    private String exchangeType;

    private String routingKey;
    private long relaxTime;
    private long timeOut;
    private Connection conn;

    private Channel channel;

    public RabbitConnectionCell(ConnectionFactory factory, String exchangeName, String exchangeType, String routingKey, long relaxTime) {
//        super();
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
        this.relaxTime = relaxTime;

        try {
            this.conn = factory.newConnection();
            this.channel = conn.createChannel();
            this.channel.exchangeDeclare(exchangeName, exchangeType);
//            channel.queuePurge(exchangeName);

        } catch (IOException | TimeoutException e) {
            log.info("fail connecting to rabbit : {0}", e);
        }
    }

    public String sendAndReceive(String message) {
        log.info("begin sending and receiving message: {}", message);
        String result = null;
        final String corrId = UUID.randomUUID().toString();

        try {

            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(corrId)
                    .replyTo(RabbitConnectionPoolConfig.REPLY_TO)
                    .build();

            // send message
            this.channel.basicPublish(exchangeName, routingKey, props, message.getBytes(StandardCharsets.UTF_8));

            // receive message
            final CompletableFuture<String> response = new CompletableFuture<>();

            String ctag = this.channel.basicConsume(RabbitConnectionPoolConfig.REPLY_TO, true, (consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                    response.complete(new String(delivery.getBody(), StandardCharsets.UTF_8));
                }
            }, consumerTag -> {
            });

            result = response.get();
            this.channel.basicCancel(ctag);
            log.info("successful send and receive");

        } catch (IOException | InterruptedException | ExecutionException e) {
            log.info("fail to send and receive message: {0}", e);
        }
        return result;
    }

    public boolean isTimeOut() {
        if (System.currentTimeMillis() - this.relaxTime > this.timeOut) {
            return true;
        }
        return false;
    }

    public void close() {
        try {
            this.channel.close();
            this.conn.close();
        } catch (Exception e) {
            log.warn("connection is closed: {0}", e);
        }
    }

    public boolean isClosed() throws Exception {
        return !conn.isOpen();
    }

}
