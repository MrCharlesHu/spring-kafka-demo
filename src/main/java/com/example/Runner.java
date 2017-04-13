package com.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

@Component
public class Runner implements CommandLineRunner, Ordered {
    @Autowired
    private Sender sender;

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; i < 5; i++) {
            sender.sendMessage(Topics.DEFAULT, "Hello world, this is spring-kafka-demo");
        }
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
