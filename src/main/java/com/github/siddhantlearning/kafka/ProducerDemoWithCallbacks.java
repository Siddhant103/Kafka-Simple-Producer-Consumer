package com.github.siddhantlearning.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {
    public static void main(String[] args) {
        System.out.println("Hello Kafka World !!!");

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);
        //Steps:
        /*
        Create Producer Properties
         */
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//kafka 2.0 -> so we can set to 5. Use 1 otherwise

        //HIgh throughput settings at the expense of a bit of latency and CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        /*
        Create a Producer<String, String> as key and value both are strings
         */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        /*
        Create a producer record, topic name and string to put into kafka
         */
        for (int i = 0; i < 10; i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World!!!" + Integer.toString(i));

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
        });
    }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
