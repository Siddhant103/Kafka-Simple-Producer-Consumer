package com.github.siddhantlearning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello Kafka World !!!");

        //Steps:
        /*
        Create Producer Properties
         */
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*
        Create a Producer<String, String> as key and value both are strings
         */
        KafkaProducer<String, String > kafkaProducer = new KafkaProducer<String, String>(properties);

        /*
        Create a producer record, topic name and string to put into kafka
         */
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World!!!");

        /*
        async call
         */
        kafkaProducer.send(record);

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
