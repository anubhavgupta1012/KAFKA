package io.kafka.kafkainit.kafkaTemplate.config;

import io.kafka.kafkainit.controller.pojo.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TemplateConsumer {

    @KafkaListener(topics = {"programatic-topic"})
    public void consumeMessages(ConsumerRecord<String, Employee> consumerRecord) {
        System.out.println("ANUBHAV Consumer +\t" + consumerRecord.value());
    }
}
