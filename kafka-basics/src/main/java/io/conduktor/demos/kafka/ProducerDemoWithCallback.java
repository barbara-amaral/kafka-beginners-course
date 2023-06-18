package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka producer");

        //1. Create Producer Properties

        Properties properties = new Properties();

        // connect to localhost

//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to conduktor playground

        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2FF8TdtmmpGMPTt7ZnjCT3\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyRkY4VGR0bW1wR01QVHQ3Wm5qQ1QzIiwib3JnYW5pemF0aW9uSWQiOjc0MDQxLCJ1c2VySWQiOjg2MTE5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI0NzljNDgwNS0wMTEyLTRkYWUtOTBkMi04MWJmMGQwZWYwMTAifX0.R0gvjJXn9Txwgzlj9Xn-yi3UswCjfTdxEur_ZyG8iUk\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // set producer properties

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // the settings commented below are for learning  and demonstration purposes only,
        // it is not recommended to use these configurations on production

        //properties.setProperty("batch.size", "400");
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //2. Create the Producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<10; j++) {
            for(int i=0; i<30; i++) {
                // Create a producer record

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World!" + i);

                //3. Send the data

                producer.send(producerRecord, (metadata, e) -> {
                    // Executes every time a record is successfully sent or an exception is thrown

                    if (e == null){
                        // the record was successfully sent
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    }else {
                        log.error("Error while producing", e);
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        for(int i=0; i<30; i++) {
            // Create a producer record

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World!" + i);

            //3. Send the data

            producer.send(producerRecord, (metadata, e) -> {
                // Executes every time a record is successfully sent or an exception is thrown

                if (e == null){
                    // the record was successfully sent
                    log.info("Received new metadata \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                }else {
                    log.error("Error while producing", e);
                }
            });
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        //4. Flush and close the Producer
        producer.close();
    }
}
