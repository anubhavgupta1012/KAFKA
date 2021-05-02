package io.kafka.kafkainit.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Value("${bootstrap-servers}")
    private String bootStrapServer;

    public static Logger logger = getLogger(KafkaController.class);
    Properties props = new Properties();


    @RequestMapping(value = "/send", method = RequestMethod.GET)
    public String getString(@RequestParam String param) {

        //Creating a propery object
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create a producer record
        // ProducerRecord record = new ProducerRecord("FIRST", param);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("FIRST", param);

        //Creating a Producer
        // KafkaProducer producer = new KafkaProducer(props);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //Sending the message
        producer.send(record);
        producer.flush();
        return "SENT";
    }
}
