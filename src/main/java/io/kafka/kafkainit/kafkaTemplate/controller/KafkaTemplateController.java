package io.kafka.kafkainit.kafkaTemplate.controller;

import io.kafka.kafkainit.controller.pojo.Employee;
import io.kafka.kafkainit.kafkaTemplate.kafkaTemplateService.TemplateProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class KafkaTemplateController {

    @Autowired
    private TemplateProducerService templateProducerService;

    @PostMapping("/template/employee")
    public void publish(@RequestBody Employee employee) throws ExecutionException, InterruptedException {
        templateProducerService.publishMessages(employee);
    }
}
