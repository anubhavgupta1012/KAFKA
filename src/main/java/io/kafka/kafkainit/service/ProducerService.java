package io.kafka.kafkainit.service;

import io.kafka.kafkainit.controller.pojo.Employee;

import java.util.Properties;

public interface ProducerService {

    String produce(String param, Properties producerProperties);

    String sendObject(Employee employee, Properties producerProperties);
}
