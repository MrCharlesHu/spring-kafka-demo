package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

//    public void sendMessage(String topic, String message) {
//        // the KafkaTemplate provides asynchronous send methods returning a Future
//         ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
//
//        // you can register a callback with the listener to receive the result of the send asynchronously
//        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
//            @Override
//            public void onSuccess(SendResult<String, String> result) {
//                LOGGER.info("Send Kafka Record >> topic={} partition={} offset={} key={} message={}",
//                        result.getRecordMetadata().topic(),
//                        result.getRecordMetadata().partition(),
//                        result.getRecordMetadata().offset(),
//                        result.getProducerRecord().key(),
//                        result.getProducerRecord().value());
//            }
//
//            @Override
//            public void onFailure(Throwable ex) {
//                LOGGER.error("unable to send message='{}'", message, ex);
//            }
//        });
//        // alternatively, to block the sending thread, to await the result, invoke the future's get() method
//    }

    public void sendMessage(String topic, String message) {
        String key0 = "filter", key1 = "values", key2 = "hotspot";
        ListenableFuture<SendResult<String, String>> future0 = kafkaTemplate.send(topic, key0, key0 + "-" + message);
        ListenableFuture<SendResult<String, String>> future1 = kafkaTemplate.send(topic, key1, key1 + "-" + message);
        ListenableFuture<SendResult<String, String>> future2 = kafkaTemplate.send(topic, key2, key2 + "-" + message);
        processResult(future0);
        processResult(future1);
        processResult(future2);
    }

    public void processResult(ListenableFuture<SendResult<String, String>> future) {
        // you can register a callback with the listener to receive the result of the send asynchronously
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.info("Send Kafka Record >> topic={} partition={} offset={} key={} message={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        result.getProducerRecord().key(),
                        result.getProducerRecord().value());
            }

            @Override
            public void onFailure(Throwable ex) {
                // LOGGER.error("unable to send message='{}'", message, ex);
                LOGGER.error("unable to send message.", ex);
            }
        });
    }
}