package vn.vnpay.runnable;

import vn.vnpay.models.PaymentRequest;
import vn.vnpay.util.GsonSingleton;
import vn.vnpay.util.KafkaUtils;

import java.util.concurrent.Callable;

public class SendPaymentCallable implements Callable<String> {

    private PaymentRequest paymentRequest;

    public SendPaymentCallable(PaymentRequest paymentRequest) {
        this.paymentRequest = paymentRequest;
    }

    @Override
    public String call() throws Exception {
        String data = GsonSingleton.toJson(paymentRequest);
        String answer = KafkaUtils.sendAndReceive(data);
        return answer;
    }
}
