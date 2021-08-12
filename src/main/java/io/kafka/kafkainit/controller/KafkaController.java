package io.kafka.kafkainit.controller;

import io.kafka.kafkainit.controller.pojo.Employee;
import io.kafka.kafkainit.controller.pojo.ProducerMessage;
import io.kafka.kafkainit.service.ConsumerService;
import io.kafka.kafkainit.service.impl.ProducerServiceImpl;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import static org.slf4j.LoggerFactory.getLogger;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private ProducerServiceImpl producerService;

    @Autowired
    private ConsumerService consumerService;

    public static Logger logger = getLogger(KafkaController.class);

    @RequestMapping(value = "/topic/{topic}", method = RequestMethod.POST)
    public String send(@PathVariable("topic") String topic, @RequestBody ProducerMessage message) {
        return producerService.produce(topic, message);
    }

    @RequestMapping(value = "/topic/{topic}", method = RequestMethod.GET)
    public String consume(@PathVariable("topic") String topic) {
        return consumerService.consume(topic);
    }

    @PostMapping(value = "/employee")
    public String sendObject(@RequestBody Employee employee) {
        return producerService.sendObject(employee);
    }

    @GetMapping(value = "/employee")
    public String consumeObject() {
        return consumerService.consumeObject();
    }
}
