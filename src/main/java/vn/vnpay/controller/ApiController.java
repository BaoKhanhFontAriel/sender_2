package vn.vnpay.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.vnpay.service.ApiService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;


@Path("/api")
public class ApiController {
    private static final Logger log = LoggerFactory.getLogger(ApiController.class);
    private ApiService apiService = new ApiService();

    @Path("/hello-world")
    @GET
    @Produces("text/plain")
    public String hello() {
        return "Hello, World!";
    }

    @Path("/sendtocore")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces("application/json")
    public String sendToCore(String data) {
//        log.info("IP call request is: {}", requestWrapper.getRemoteUser());

        log.info("sending data is: {}", data);

        long start = System.currentTimeMillis();
        String message = apiService.sendToCore(data);
        long end = System.currentTimeMillis();

        log.info("end - start: {}", end - start);
        log.info("Time from api request to response is: {} ms", end - start);
        return message;
    }
    @Path("/sendtocore2")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces("application/json")
    public String sendToCore2(String data) {
//        log.info("IP call request is: {}", requestWrapper.getRemoteUser());

        log.info("sending data is: {}", data);

        long start = System.currentTimeMillis();
        String message = apiService.sendToCore2(data);
        long end = System.currentTimeMillis();

        log.info("end - start: {}", end - start);
        log.info("Time from api request to response is: {} ms", end - start);
        return message;
    }


    @Path("/sendpayment")
    @GET
    @Produces("application/json")
    public String sendPayment() {
//        log.info("IP call request is: {}", request.getRemoteAddr());
//        log.info("sending data is: {}", data);

        long start = System.currentTimeMillis();
        String message = apiService.sendPayment();
        long end = System.currentTimeMillis();

        log.info("end - start: {}", end - start);
        log.info("Time from api request to response is: {} ms", end - start);
        return message;
    }
}