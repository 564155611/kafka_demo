package com.fanx.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
@Slf4j
public class Consumer {
    public static final String BROKER_LIST = "192.168.12.171:9092,192.168.12.172:9092,192.168.12.173:9092";
    public static final String TOPIC = "topic-demo";
    public static final String GROUP_ID = "group.demo";
    public static final String CLIENT_ID = "consumer.client.id.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        KafkaConsumer<String, Company> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, Company> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Company> record : records) {
                    System.out.println("topic = " + record.topic() +
                            ",partition = " + record.partition() +
                            ",offset = " + record.offset());
                    System.out.println("key = " + record.key() +
                            ", value = " + record.value());
                }
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}
