package io.kafka.kafkainit.service;

import java.util.Properties;

public interface ProducerService {

    String produce(String param, Properties producerProperties);
}
