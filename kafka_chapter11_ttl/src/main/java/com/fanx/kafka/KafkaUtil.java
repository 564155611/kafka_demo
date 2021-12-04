package com.fanx.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaUtil {
    public static final String BROKER_LIST = "localhost:9092";
    public static final String TOPIC = "topic-demo";
    /*消费者相关*/
    public static final String GROUP_ID = "group.demo";
    public static final String CLIENT_ID = "consumer.client.id.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties producerConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return properties;
    }

    public static KafkaProducer<String, String> buildProducer(Properties properties) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    public static Properties consumerConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, CLIENT_ID);
        return properties;
    }

    public static KafkaConsumer<String, String> buildConsumer(Properties properties) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(TOPIC));
        return consumer;
    }

    public static ProducerRecord<String,String> producerRecord(String key, String value, Map<String,Object> headersMap){
        Long timestamp = null;
        Integer partition = null;
        final RecordHeaders headers = new RecordHeaders();
        for (Map.Entry<String, Object> entry : headersMap.entrySet()) {
            String k = entry.getKey();
            Object v = entry.getValue();
            if (k == null) {
                continue;
            }
            if ("timestamp".equals(k)) {
                timestamp = (Long) v;
            }else if ("partition".equals(k)){
                partition = (Integer) v;
            }else{
                byte[] bytes = SerdeserializeUtil.serialize(v);
                RecordHeader recordHeader = new RecordHeader(k, bytes);
                headers.add(recordHeader);
            }
        }
        return new ProducerRecord<String, String>(TOPIC, partition, timestamp, key, value, headers);
    }

    public static ProducerRecord<String,String> producerRecord(String value, Map<String,Object> headersMap){
        Long timestamp = null;
        Integer partition = null;
        final RecordHeaders headers = new RecordHeaders();
        for (Map.Entry<String, Object> entry : headersMap.entrySet()) {
            String k = entry.getKey();
            Object v = entry.getValue();
            if (k == null) {
                continue;
            }
            if ("timestamp".equals(k)) {
                timestamp = (Long) v;
            }else if ("partition".equals(k)){
                partition = (Integer) v;
            }else{
                byte[] bytes = SerdeserializeUtil.serialize(v);
                RecordHeader recordHeader = new RecordHeader(k, bytes);
                headers.add(recordHeader);
            }
        }
        return new ProducerRecord<String, String>(TOPIC, partition, timestamp, null, value, headers);
    }

    public static ProducerRecord<String,String> producerRecord(String key, String value){
        return new ProducerRecord<String, String>(TOPIC, null, null, key, value);
    }

    public static ProducerRecord<String,String> producerRecord(String value){
        long timestamp = System.currentTimeMillis();
        return new ProducerRecord<String, String>(TOPIC, null, null, null, value);
    }
}
