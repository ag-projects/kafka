package agharibi.twitter;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private static final String CONSUMER_KEY = "*********************************************************";
    private static final String CONSUMER_SECRET = "**************************************";
    private static final String TOKEN = "****************************";
    private static final String TOKEN_SECRET = "***************************************";

    private static final String TOPIC = "twitter_tweets";
    private static final String BOOTSTRAP_SERVER_VALUE = "127.0.01:9092";
    private List<String> terms = Lists.newArrayList("bitcoin", "USA", "sports");

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Setup..");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();
        shutdownHook(client, producer);

        processTweets(msgQueue, client, producer);
        logger.info("End of application..");
    }

    private void processTweets(BlockingQueue<String> msgQueue,
                               Client client,
                               KafkaProducer<String, String> producer) {

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>(TOPIC, null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("An error occurred", e);
                    }
                });
            }
        }
    }

    private void shutdownHook(Client client, KafkaProducer<String, String> producer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application..");
            logger.info("Shutting down client from twitter..");
            client.stop();
            logger.info("Closing producer..");
            producer.close();
            logger.info("Done!");
        }));
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(LINGER_MS_CONFIG, "20");
        properties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return new KafkaProducer<>(properties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint filterEndpoint = new StatusesFilterEndpoint();

        filterEndpoint.trackTerms(terms);
        Authentication authentication = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);

        ClientBuilder client = new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosts)
            .authentication(authentication)
            .endpoint(filterEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        return client.build();
    }

}
