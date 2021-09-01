package io.kafka.kafkainit.kafkaTemplate.controller;

import io.kafka.kafkainit.controller.pojo.Employee;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class KafkaTemplateControllerTest {

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Test
    public void publish() {
        Employee employee = new Employee("qopwow", "DHEERAJ");
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity httpEntity = new HttpEntity(employee, httpHeaders);
        ResponseEntity<Employee> response =
            testRestTemplate.exchange("http://localhost:8080/template/employee", HttpMethod.POST, httpEntity, Employee.class);
        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
    }
}
