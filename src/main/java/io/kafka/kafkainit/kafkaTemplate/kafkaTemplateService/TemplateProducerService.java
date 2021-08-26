package io.kafka.kafkainit.kafkaTemplate.kafkaTemplateService;

import io.kafka.kafkainit.controller.pojo.Employee;

public interface TemplateProducerService {

    void publishMessages(Employee employee);
}
