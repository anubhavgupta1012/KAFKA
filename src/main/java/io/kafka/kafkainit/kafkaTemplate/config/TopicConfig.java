package io.kafka.kafkainit.kafkaTemplate.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
public class TopicConfig {

    @Value("${kafka-topic-name}")
    private String topicName;

    /*this  type of topic creation is not encouraged in prod environment so we have used @Profile*/
    @Bean
    public NewTopic getNewTopic() {
        System.out.println("Code came Here");
        return TopicBuilder.name(topicName)
            .partitions(3)
            .replicas(1)
            .build();
    }
}
