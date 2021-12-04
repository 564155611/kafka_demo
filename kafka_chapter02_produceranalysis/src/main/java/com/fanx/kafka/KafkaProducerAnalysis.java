package com.fanx.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerAnalysis {
    public static final String BROKER_LIST = "192.168.12.171:9092,192.168.12.172:9092,192.168.12.173:9092";
    public static final String TOPIC = "topic-demo";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("client.id", "producer.client.id.demo");
        return properties;
    }

    /**
     * 使用org.apache.kafka.clients.producer.ProducerConfig类
     *
     * @return
     */
    public static Properties initConfig2() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return properties;

    }

    public static KafkaProducer<String, String> buildProducer(Properties properties) {
        return new KafkaProducer<String, String>(properties);
    }

    public static KafkaProducer<String, String> buildProducer(Properties properties, Serializer<String> keySerializer, Serializer<String> valueSerializer) {
        return new KafkaProducer<String, String>(properties, keySerializer, valueSerializer);
    }

    /**
     * 发后即忘
     *
     * @param producer
     * @param record
     */
    public static void sendFireAndForget(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步发送
     *
     * @param producer
     * @param record
     */
    public static void sendAsync(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                }
            }
        });
    }

    /**
     * 同步发送
     *
     * @param producer
     * @param record
     */
    public static void sendSync(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello, Kafka!");

        //发送消息
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
