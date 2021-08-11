package io.kafka.kafkainit.service.impl;

import io.kafka.kafkainit.controller.pojo.Employee;
import io.kafka.kafkainit.network.EmpDeserializer;
import io.kafka.kafkainit.service.ConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

@Service
public class ConsumerServiceImpl implements ConsumerService {

    public static Logger logger = getLogger(ConsumerServiceImpl.class);

    @Value("${bootstrap-servers}")
    private String bootStrapServer;

    @Value("${custom_topic}")
    private String customObjectTopic;

    @Value("${custom_topic_group}")
    private String customObjectTopicGroup;

    @Override
    public String consume() {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "GFIRST");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

        //subscribe consumer to topic
        consumer.subscribe(Collections.singleton("FIRST"));

        /*assign & seek are generally used for consuming messages from a definite TopicPartition from a definite offset
        TopicPartition topicPartition = new TopicPartition("FIRST", 0);
        consumer.assign(Collections.singleton(topicPartition));

        //seek
        long offsettoReadFrom = 20;
        consumer.seek(topicPartition,offsettoReadFrom);*/

        //poll the new Data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {

                String result = "Key : " + record.key() +
                    "\nvalue : " + record.value() +
                    "\nPartition : " + record.partition() +
                    "\n offset : " + record.offset();
                System.out.println(" RESULT :" + result);
            }
        }

    }

    @Override
    public String consumeObject() {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EmpDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, customObjectTopicGroup);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, Employee> consumer = new KafkaConsumer<String, Employee>(consumerProperties);

        //subscribe consumer to topic
        consumer.subscribe(Collections.singleton(customObjectTopic));

        /*assign & seek are generally used for consuming messages from a definite TopicPartition from a definite offset
        TopicPartition topicPartition = new TopicPartition("FIRST", 0);
        consumer.assign(Collections.singleton(topicPartition));

        //seek
        long offsettoReadFrom = 20;
        consumer.seek(topicPartition,offsettoReadFrom);*/

        //poll the new Data
        while (true) {
            ConsumerRecords<String, Employee> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Employee> record : records) {

                String result = "name : " + record.value().getName() +
                    "\nid : " + record.value().getId();
                System.out.println(" RESULT :" + result);
                Employee value = record.value();
                System.out.println("Employee\t" + value);
            }
        }
    }
}
