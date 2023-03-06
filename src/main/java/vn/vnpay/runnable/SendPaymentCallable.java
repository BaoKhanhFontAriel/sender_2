package vn.vnpay.runnable;

import vn.vnpay.models.PaymentRequest;
import vn.vnpay.util.GsonSingleton;
import vn.vnpay.util.KafkaUtils;
import vn.vnpay.util.MathUtils;
import vn.vnpay.util.TokenUtils;

import java.util.concurrent.Callable;

public class SendPaymentCallable implements Callable<String> {

    private PaymentRequest paymentRequest;

    public SendPaymentCallable(PaymentRequest paymentRequest) {
        this.paymentRequest = paymentRequest;
    }

    @Override
    public String call() throws Exception {
        KafkaUtils.send(paymentRequest.toString());
        return "success send to kafka " + paymentRequest.getRequestid();
    }
}
