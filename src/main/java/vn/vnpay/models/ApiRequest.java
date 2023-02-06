package vn.vnpay.models;


import lombok.*;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class ApiRequest {
    private String token;
    private String data;
}
