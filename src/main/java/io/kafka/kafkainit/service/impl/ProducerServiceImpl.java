package io.kafka.kafkainit.service.impl;

import io.kafka.kafkainit.controller.pojo.Employee;
import io.kafka.kafkainit.controller.pojo.ProducerMessage;
import io.kafka.kafkainit.network.EmpSerializer;
import io.kafka.kafkainit.service.ProducerService;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

@Service
public class ProducerServiceImpl implements ProducerService {

    public static Logger logger = getLogger(ProducerServiceImpl.class);

    @Value("${bootstrap-servers}")
    private String bootStrapServer;

    Properties producerProperties = new Properties();

    @Override
    public String produce(String topic, ProducerMessage message) {
        //Creating a property object
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create a producer record
        // ProducerRecord record = new ProducerRecord("FIRST", param);
        String msg = message.getMsg();
        String key = "key_" + msg.length();
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, msg);

        //Creating a Producer
        // KafkaProducer producer = new KafkaProducer(props);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        //Sending the message
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null) {
                    System.out.println(" Producer details are as follows: \n" +
                        "topic: " + metadata.topic() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "partition: " + metadata.partition() + "\n" +
                        "timestamp: " + metadata.timestamp());
                } else {
                    logger.error("Exception occurred while sending the data", e);
                }
            }
        });
        producer.close();
        return "SENT";
    }

    @Override
    public String sendObject(Employee employee) {
        //Creating a property object
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EmpSerializer.class.getName());


        //Create a producer record
        // ProducerRecord record = new ProducerRecord("FIRST", param);
        String key = "key_" + employee.toString().length();
        ProducerRecord<String, Employee> record = new ProducerRecord<String, Employee>("CUSTOM", key, employee);

        //Creating a Producer
        // KafkaProducer producer = new KafkaProducer(props);
        KafkaProducer<String, Employee> producer = new KafkaProducer<String, Employee>(producerProperties);

        //Sending the message
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null) {
                    System.out.println("RECORD details are as follows: \n" +
                        "topic: " + metadata.topic() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "partition: " + metadata.partition() + "\n" +
                        "timestamp: " + metadata.timestamp());
                } else {
                    logger.error("Exception occurred while sending the data", e);
                }
            }
        });
        producer.close();
        return "SENT";
    }
}
