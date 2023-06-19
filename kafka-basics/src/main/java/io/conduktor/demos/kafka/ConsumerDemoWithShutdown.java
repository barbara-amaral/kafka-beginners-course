package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a kafka consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        //1. Create Producer Properties

        Properties properties = new Properties();

        // connect to localhost

//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to conduktor playground

        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2FF8TdtmmpGMPTt7ZnjCT3\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyRkY4VGR0bW1wR01QVHQ3Wm5qQ1QzIiwib3JnYW5pemF0aW9uSWQiOjc0MDQxLCJ1c2VySWQiOjg2MTE5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI0NzljNDgwNS0wMTEyLTRkYWUtOTBkMi04MWJmMGQwZWYwMTAifX0.R0gvjJXn9Txwgzlj9Xn-yi3UswCjfTdxEur_ZyG8iUk\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // create consumer configs

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        // none: if there is no consumer groups, it will fail
        // earliest: read from the beginning of my topic
        // latest: reads only new messages starting from now

        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread

        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            //subscribe for a topic
            consumer.subscribe(Arrays.asList(topic));

            // poll for data

            while (true) {
                //log.info("Polling...");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key" + record.key() + ", Value: " + record.value());
                    log.info("Partition" + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        }catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        }finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shutdown");       }
    }
}
