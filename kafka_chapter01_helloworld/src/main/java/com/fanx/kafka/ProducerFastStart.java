package com.fanx.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerFastStart {
    public static final String BROKER_LIST = "localhost:9092";
    public static final String TOPIC = "topic-monitor";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", BROKER_LIST);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //发送消息
        try {
            for (int i = 1; i <= 100000; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, i+"====hello, Kafka!");
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
