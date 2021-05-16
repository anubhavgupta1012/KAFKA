package io.kafka.kafkainit.twitterTest;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

public class TestConsumerElasticSearch {

    static Logger logger = LoggerFactory.getLogger(TestConsumerElasticSearch.class);

    public static void main(String[] args) throws IOException {
        RestHighLevelClient elasticSearchClient = getElasticSearchClient();

        String data = "{\n" +
            "\"name\" : \"DHEERAJ KUMAR GUPTA\",\n" +
            "\"age\" : 24,\n" +
            "\"topic\" : \"elastic search learning\"\n" +
            "}";

        IndexRequest indexRequest = new IndexRequest("twitter", "randomData")
            .source(data, XContentType.JSON);
        IndexResponse indexResponse = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        int status = indexResponse.status().getStatus();
        String index = indexResponse.getIndex();

        logger.info("id :{}\n  status:{}\n  index:{}\n", id, status, index);

        //closing the client
        elasticSearchClient.close();
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
}