package agharibi.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private static final String GROUP_ID = "my-fifth-app";
    private static final String FIRST_TOPIC = "first_topic";
    private static Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static final String BOOTSTRAP_SERVER_VALUE = "127.0.01:9092";

    public static void main(String[] args) {

        Properties pros = new Properties();
        pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        pros.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pros.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pros.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        pros.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);
        consumer.subscribe(Arrays.asList(FIRST_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                logger.info("key: " + record.key() + "\n value: " + record.value() + "\n partition: " + record.partition() + "\n offset: " + record.offset());
            });
        }

    }
}
