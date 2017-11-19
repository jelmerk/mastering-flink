package com.demo.flink.producing;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());


        while(true) {
            producer.send(new ProducerRecord<>("temp", "sensor1",
                    System.currentTimeMillis() + ",100,sensor1"));

            Thread.sleep(2000);

            producer.send(new ProducerRecord<>("temp", "sensor1",
                    System.currentTimeMillis() + ",50,sensor1"));

            Thread.sleep(2000);
        }
    }
}
