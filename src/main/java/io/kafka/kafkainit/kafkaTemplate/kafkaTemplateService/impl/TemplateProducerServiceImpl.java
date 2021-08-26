package io.kafka.kafkainit.kafkaTemplate.kafkaTemplateService.impl;

import io.kafka.kafkainit.controller.pojo.Employee;
import io.kafka.kafkainit.kafkaTemplate.kafkaTemplateService.TemplateProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class TemplateProducerServiceImpl implements TemplateProducerService {

    @Value("${kafka-topic-name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;

    @Override
    public void publishMessages(Employee employee) {
        ListenableFuture<SendResult<String, Employee>> result = kafkaTemplate.send(topicName, employee);
        result.addCallback(new ListenableFutureCallback<SendResult<String, Employee>>() {
            @Override
            public void onFailure(Throwable throwable) {
                System.out.println(throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Employee> result1) {
                System.out.println(result1.getProducerRecord().value());
            }
        });
    }
}
