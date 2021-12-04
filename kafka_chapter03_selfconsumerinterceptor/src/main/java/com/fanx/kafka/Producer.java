package com.fanx.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static final String BROKER_LIST = "192.168.12.171:9092,192.168.12.172:9092,192.168.12.173:9092";
    public static final String TOPIC = "topic-demo";
    public static final long EXPIRE_INTERVAL = TTLConsumerInterceptor.EXPIRE_INTERVAL;


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record1 =
                new ProducerRecord<>(
                        TOPIC,
                        0,
                        System.currentTimeMillis()-EXPIRE_INTERVAL,
                null,
                "first-expire-data");
        producer.send(record1).get();

        ProducerRecord<String, String> record2 =
                new ProducerRecord<>(
                        TOPIC,
                        0,
                        System.currentTimeMillis(),
                null,
                "normal-data");
        producer.send(record2).get();

        ProducerRecord<String, String> record3 =
                new ProducerRecord<>(
                        TOPIC,
                        0,
                        System.currentTimeMillis()-EXPIRE_INTERVAL,
                null,
                "last-expire-data");
        producer.send(record3).get();


        producer.close();
    }
}
