package com.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private final CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(topics = Topics.IPTV_LOG)
    public void receiveMessage(String message) {
        LOGGER.info("received message='{}'", message);
        latch.countDown();
    }

    @KafkaListener(topics = Topics.REPORT_REQUEST)
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        System.out.println(String.format("Offset: %s, Key: %s, Value: %s, Timestamp: %s",
                cr.offset(), cr.key(), cr.value(), cr.timestamp()));
        Thread.sleep(1000 * 15);
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

}
