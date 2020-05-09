package agharibi.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignAndSeek {

    private static final String FIRST_TOPIC = "first_topic";
    private static Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static final String BOOTSTRAP_SERVER_VALUE = "127.0.01:9092";

    public static void main(String[] args) {

        Properties pros = new Properties();
        pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        pros.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pros.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pros.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(pros);

        // assign
        Long offsetToReadFrom = 15L;
        TopicPartition topicPartitionToReadFrom = new TopicPartition(FIRST_TOPIC, 0);
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));

        // seek
        consumer.seek(topicPartitionToReadFrom, offsetToReadFrom);

        int numberOfMessageToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;


        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records) {
                numberOfMessagesReadSoFar =+1;
                logger.info("key: " + record.key() +
                "\n value: " + record.value() +
                "\n partition: " + record.partition() +
                "\n offset: " + record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessageToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting from app..");

    }
}
