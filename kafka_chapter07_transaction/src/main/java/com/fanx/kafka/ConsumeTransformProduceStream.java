package com.fanx.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class ConsumeTransformProduceStream {
    public static final String BROKER_LIST = "192.168.12.171:9092,192.168.12.172:9092,192.168.12.173:9092";
    public static final String TOPIC_SOURCE = "topic-source";
    public static final String TOPIC_SINK = "topic-sink";
    public static final String GROUP_ID = "groupId";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(TOPIC_SOURCE));
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(getProducerProperties());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        //初始化事务
        producer.initTransactions();

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            if (!records.isEmpty()) {
                producer.beginTransaction();
                try {
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            //TODO do some logical processing before produce
                            ProducerRecord<String, String> producerRecord =
                                    new ProducerRecord<>(TOPIC_SINK, record.key(), record.value());
                            //消费-生产模型
                            producer.send(producerRecord);
                        }
                        long lastConsumedOffset = partitionRecords
                                .get(partitionRecords.size() - 1).offset();
                        offsets.put(partition,new OffsetAndMetadata(lastConsumedOffset+1));
                    }
                    //提交消费位移到事务控制中
                    producer.sendOffsetsToTransaction(offsets,GROUP_ID);
                    //提交事务
                    producer.commitTransaction();
                } catch (ProducerFencedException e) {
                    e.printStackTrace();
                    producer.abortTransaction();
                }
            }
        }


    }

    public static Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return props;
    }

    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return props;
    }
}
