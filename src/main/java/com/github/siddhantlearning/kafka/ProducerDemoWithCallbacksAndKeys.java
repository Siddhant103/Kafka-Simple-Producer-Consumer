package com.github.siddhantlearning.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallbacksAndKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello Kafka World !!!");

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacksAndKeys.class);
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
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        /*
        Create a producer record, topic name and string to put into kafka
         */
        for (int i = 0; i < 10; i++){
            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            /*
            async call
             */
        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes everytime a record is successfully sent or an exception is thrown
                if (e == null) {
                    logger.info("Received Metadata. \n " +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while placing record: " + e.getMessage());
                }
            }
        }).get(); //Blocking call - Synchronous - Should not be done in prod since it produces bad performance.
    }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
