package io.kafka.kafkainit.service;

public interface ConsumerService {

    String consume(String topic);

    String consumeObject();
}
