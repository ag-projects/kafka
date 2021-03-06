package agharibi.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerKeys {


    private static Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
    private static final String BOOTSTRAP_SERVER_VALUE = "127.0.01:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Kafka regards " + Integer.valueOf(i);
            String key = "id_" + Integer.valueOf(i);

            ProducerRecord producerRecord = new ProducerRecord<>(topic, key, value);

            logger.info("key: " + key);

            producer.send(producerRecord, (recordMetadata, e) ->
                logger.info("\n Received metadata -> \n " +
                "Topic: " + recordMetadata.topic() + "\n " +
                "Partition: " + recordMetadata.partition() + "\n " +
                "Offset: " + recordMetadata.offset() + "\n " +
                "Timestamp: " + recordMetadata.timestamp())).get();
        }

        producer.flush();
        producer.close();

    }
}
