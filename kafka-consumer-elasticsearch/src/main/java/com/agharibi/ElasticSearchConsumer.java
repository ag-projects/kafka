package com.agharibi;


import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
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
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final String TOPIC = "twitter_tweets";
    private static final String GROUP_ID = "kafka-demo-elasticsearch";
    private static final String BOOTSTRAP_SERVER_VALUE = "127.0.01:9092";

    private static final int PORT = 443;
    private static final String SCHEME = "https";
    private static final String username = "***************";
    private static final String password = "***************";
    private static final String hostname = "***************************";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private static JsonParser jsonParser = new JsonParser();

    public static RestHighLevelClient createClient() {

        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient
            .builder(new HttpHost(hostname, PORT, SCHEME))
            .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        Properties pros = new Properties();
        pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        pros.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pros.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pros.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        pros.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer(TOPIC);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {

                String id = extractTwitterId(record.value());
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id);
                indexRequest.source(record.value(), XContentType.JSON);

                IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("Response id: " + response.getId());
                Thread.sleep(1000);
            }
        }
    }

    private static String extractTwitterId(String tweet) {
        return jsonParser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
    }
}
