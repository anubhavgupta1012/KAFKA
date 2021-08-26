package io.kafka.kafkainit.kafkaTemplate.kafkaTemplateService;

import io.kafka.kafkainit.controller.pojo.Employee;

import java.util.concurrent.ExecutionException;

public interface TemplateProducerService {

    void publishMessages(Employee employee) throws ExecutionException, InterruptedException;
}
