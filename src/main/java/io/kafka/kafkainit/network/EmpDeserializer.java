package io.kafka.kafkainit.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafka.kafkainit.controller.pojo.Employee;
import org.apache.catalina.User;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class EmpDeserializer implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Employee user = null;
        try {
            user = mapper.readValue(data, Employee.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return user;
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
    }
}
