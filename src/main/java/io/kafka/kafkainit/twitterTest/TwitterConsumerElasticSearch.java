package io.kafka.kafkainit.twitterTest;

import com.google.gson.JsonParser;
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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
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
            logger.info("{} number of records are polled", records.count());
            BulkRequest bulkRequest = new BulkRequest();
            if (records.count() > 0) {
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        IndexRequest indexRequest = new IndexRequest("twitter", "randomData", getTweetId(record.value()))
                            .source(record.value(), XContentType.JSON);
                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {
                        logger.error("exception occurred while pushing the data to elastic search :{}", e);
                    }
                }
                try {
                    BulkResponse bulkResponse = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    logger.error("exception occurred while pushing data in bulk form :{}", e);
                }
                kafkaConsumer.commitSync();
                logger.info("committed offset");
            }
        }
        //closing the client
        //elasticSearchClient.close();
    }

    private static String getTweetId(String tweet) {
        return JsonParser.parseString(tweet).getAsJsonObject().get("id_str").toString();
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
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit offset

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        //subscribe consumer to topic
        consumer.subscribe(topics);
        return consumer;
    }
}