package vn.vnpay.models;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@ToString
@Getter
public class ApiResponse {
    private String resCode;
    private String message;
    private String data;


}
