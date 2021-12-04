package com.fanx.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionalProducer {
    public static final String BROKER_LIST = "192.168.12.171:9092,192.168.12.172:9092,192.168.12.173:9092";
    public static final String TOPIC = "topic-demo";
    public static final String TRANSACTIONAL_ID = "transactionalId";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        producer.initTransactions();
        producer.beginTransaction();

        try {
            //TODO 处理业务逻辑并创建ProducerRecord
            ProducerRecord<String, String> record1 = new ProducerRecord<>(TOPIC, "msg1");
            producer.send(record1);
            ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC, "msg2");
            producer.send(record2);
            ProducerRecord<String, String> record3 = new ProducerRecord<>(TOPIC, "msg3");
            producer.send(record3);
            //TODO 处理一些其他逻辑
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            producer.abortTransaction();
        }

        producer.close();
    }
}
