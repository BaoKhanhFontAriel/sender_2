package vn.vnpay.models;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class PaymentRequest {
    private String topic;
    private String server;
    private String response;
    private String uri;
    private int httpStatus;
    private String code;
    private String mid ;

    private String deviceType;
    private String requestid;
    private int processtime;
    private String[] client_ip;
    private String device;
    private String user;

    @Override
    public String toString() {
        return "mam,server=10.20.27.21,response=success,uri=/payment,http_status=200,code=00,mid=00,device_type=Linux requestid=\""+requestid+"\",processtime="+processtime+"i,client_ip=\"10.20.27.21, 10.20.27.21\",device=\"device\",user=\"user\"";
    }
}
