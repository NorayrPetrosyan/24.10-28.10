package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        /**
         * create Producer properties
         */
        Properties properties = new Properties();

        // String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * create the producer
         */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        /**
         * create a producer record
         */
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        /**
         * send data - asynchronous
         * Asynchronous call does not block the program from the code execution.
         * When the call returns from the event, the call returns to the callback function.
         */
        producer.send(record);

        // flush data
        producer.flush();
        // flush and close the producer
        producer.close();

    }
}