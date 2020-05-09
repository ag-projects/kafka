package agharibi.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreads {

    private static final String GROUP_ID = "my-sixth-app";
    private static final String FIRST_TOPIC = "first_topic";
    private static final String BOOTSTRAP_SERVER_VALUE = "127.0.01:9092";
    Logger logger = LoggerFactory.getLogger(ConsumerWithThreads.class.getName());

    public static void main(String[] args) {
        ConsumerWithThreads consumer = new ConsumerWithThreads();
        consumer.run();
    }

    public void run() {
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread consumerRunnable = new ConsumerThread(BOOTSTRAP_SERVER_VALUE, GROUP_ID, FIRST_TOPIC, latch);

        Thread t1 = new Thread(consumerRunnable);
        t1.start();

        Runtime.getRuntime().addShutdownHook(new Thread(
            () -> {
                logger.info("Caught shutdown hook");
                consumerRunnable.shutdown();

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("app has exited");
            }
        ));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application is interrupted..", e);
        } finally {
            logger.info("Application is closing..");
        }
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        Logger log = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(String bootstrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch) {

            this.latch = latch;

            Properties pros = new Properties();
            pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            pros.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            pros.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            pros.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            pros.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(pros);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(record -> log.info("key: " + record.key() +
                        "\n value: " + record.value() +
                        "\n partition: " + record.partition() +
                        "\n offset: " + record.offset()));
                }
            } catch (WakeupException e) {
                log.info("Received shutdown signal..");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            // interrupts consumer.poll();
            consumer.wakeup();
        }
    }
}
