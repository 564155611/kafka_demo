package com.fanx.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerClient {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer =
                KafkaUtil.buildConsumer(KafkaUtil.consumerConfig());

    }
}
