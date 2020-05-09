package agharibi.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class Producer {

    public static final String BOOTSTRAP_SERVER_VALUE = "127.0.01:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<String, String>(properties);
        ProducerRecord producerRecord = new ProducerRecord<String, String>("first_topic", "Hello Kafka");

        producer.send(producerRecord);
        producer.flush();
        producer.close();

    }
}
