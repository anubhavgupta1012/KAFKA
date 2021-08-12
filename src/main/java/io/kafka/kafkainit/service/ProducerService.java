package io.kafka.kafkainit.service;

import io.kafka.kafkainit.controller.pojo.Employee;
import io.kafka.kafkainit.controller.pojo.ProducerMessage;

public interface ProducerService {

    String produce(String param, ProducerMessage message);

    String sendObject(Employee employee);
}
