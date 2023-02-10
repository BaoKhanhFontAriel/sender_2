package vn.vnpay.util;

import lombok.extern.slf4j.Slf4j;
import vn.vnpay.models.ApiRequest;

@Slf4j
public class RequestUtils {
    public static ApiRequest createRequest(String data) {
        String token = TokenUtils.generateNewToken();
        ApiRequest customerRequest = new ApiRequest(token, data);
        log.info("create request {}", customerRequest.toString());
        return customerRequest;
    }
}
