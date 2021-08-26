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

import java.util.concurrent.ExecutionException;

@Service
public class TemplateProducerServiceImpl implements TemplateProducerService {

    @Value("${kafka-topic-name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;

    @Override
    public void publishMessages(Employee employee) throws ExecutionException, InterruptedException {
        //it will return a Future object and is Async. Statement below this line will be called irrespective of the
        // value inside this future object
        ListenableFuture<SendResult<String, Employee>> result = kafkaTemplate.send(topicName, employee);

        //while this will result Result object & is Sync. Statement below this line won't be called
        // until kafkaTemplate.send(topicName, employee).get() will be calculated.
        /*SendResult<String, Employee> stringEmployeeSendResult = kafkaTemplate.send(topicName, employee).get();*/
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
