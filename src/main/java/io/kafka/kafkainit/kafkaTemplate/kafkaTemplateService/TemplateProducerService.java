package io.kafka.kafkainit.kafkaTemplate.kafkaTemplateService;

import io.kafka.kafkainit.controller.pojo.Employee;
import org.springframework.http.ResponseEntity;

import java.util.concurrent.ExecutionException;

public interface TemplateProducerService {

    ResponseEntity<Employee> publishMessages(Employee employee) throws ExecutionException, InterruptedException;
}
