package io.kafka.kafkainit.twitterTest;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private String key = "jauwYDMBLkwlJf7GSwXKH4wqZ";
    private String secret = "0HHu5jVzRoLURn209Bk4v0SEMDevGttz0lgYPFxYCGSnzIQXIP";
    private String token = "3988673065-ydNIkjSUZPnBhP73vwMJXVd1cJciw5lYIPjVpEU";
    private String tokenSecret = "UrfQ2JmLJIjgUdLfNYQBSCPMzMvs7iGmZHlamm4UMK645";

    public static void main(String[] args) {
        new TwitterProducer().tweet();
    }

    public void tweet() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10);

        //creating a twitter client
        Client twitterClient = getTwitterClient(msgQueue);

        //establish a connection
        twitterClient.connect();

        //create kafka producer
        KafkaProducer<String, String> kafkaProducer = getKafkaProducer();


        String msg = null;
        while (!twitterClient.isDone()) {
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error("Error occurred while printing the msgs " + e);
                twitterClient.stop();
            }
            if (msg != null) {
                System.out.println(msg);
                kafkaProducer.send(new ProducerRecord<String, String>("FIRST", msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Error occurred while producing the msg to Kafka\n", e);
                        }
                    }
                });
            }
        }
        logger.info(" Happy Ending");
    }

    public Client getTwitterClient(BlockingQueue<String> msgQueue) {
        //create a twitterClient
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("qwerty654asdfre ");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(key, secret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();

    }

    private KafkaProducer<String, String> getKafkaProducer() {
        Properties producerProperties = new Properties();
        //Creating a propery object
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating a Producer
        return new KafkaProducer<String, String>(producerProperties);

    }
}
