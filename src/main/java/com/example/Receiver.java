package com.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private final CountDownLatch latch = new CountDownLatch(1);

    // @KafkaListener(topics = Topics.IPTV_LOG)
    // public void receiveMessage(String message) {
    //    LOGGER.info("received message='{}'", message);
    //    latch.countDown();
    //}

    // @KafkaListener(topics = Topics.REPORT_REQUEST)
    @KafkaListener(topics = Topics.DEFAULT)
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        LOGGER.info("Receive Kafka Record >> topic={} partition={} offset={} key={} value={}",
                cr.topic(), cr.partition(), cr.offset(), cr.key(), cr.value());
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

}
