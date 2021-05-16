package io.kafka.kafkainit.twitterTest;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class TwitterConsumerElasticSearch {
    static Logger logger = LoggerFactory.getLogger(TwitterConsumerElasticSearch.class);

    public static void main(String[] args) {
        RestHighLevelClient elasticSearchClient = getElasticSearchClient();
        KafkaConsumer kafkaConsumer = getKafkaConsumer(Collections.singleton("FIRST"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    IndexRequest indexRequest = new IndexRequest("twitter", "randomData")
                        .source(record.value(), XContentType.JSON);
                    IndexResponse indexResponse = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();
                    logger.info("\nDHEERAJ id :{}", id);
                    Thread.sleep(1000);
                } catch (InterruptedException | IOException e) {
                    logger.error("exception occurred while pushing the data to elastic search :{}", e);
                }
            }
        }
        //closing the client
        //elasticSearchClient.close();
    }

    private static RestHighLevelClient getElasticSearchClient() {
        String host = "kafka-tweets-9365510487.ap-southeast-2.bonsaisearch.net";
        String username = "gznvcx14eh";
        String password = "ey2wj6dlt5";
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(host, 443, "https"))
            .setHttpClientConfigCallback((httpAsyncClientBuilder) -> {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        return new RestHighLevelClient(restClientBuilder);
    }

    private static KafkaConsumer getKafkaConsumer(Collection topics) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "GFIRST");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        //subscribe consumer to topic
        consumer.subscribe(topics);
        return consumer;
    }
}