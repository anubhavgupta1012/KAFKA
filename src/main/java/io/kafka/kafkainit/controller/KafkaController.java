package io.kafka.kafkainit.controller;

import io.kafka.kafkainit.controller.pojo.Employee;
import io.kafka.kafkainit.service.ConsumerService;
import io.kafka.kafkainit.service.impl.ProducerServiceImpl;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private ProducerServiceImpl producerService;

    @Autowired
    private ConsumerService consumerService;

    public static Logger logger = getLogger(KafkaController.class);
    Properties producerProperties = new Properties();

    @RequestMapping(value = "/send", method = RequestMethod.GET)
    public String send(@RequestParam String param) {
        return producerService.produce(param, producerProperties);
    }

    @RequestMapping(value = "/consume", method = RequestMethod.GET)
    public String consume() {
        return consumerService.consume();
    }

    @PostMapping(value = "/employee")
    public String sendObject(@RequestBody Employee employee) {
        return producerService.sendObject(employee, producerProperties);
    }

    @GetMapping(value = "/employee")
    public String consumeObject() {
        return consumerService.consumeObject();
    }
}
