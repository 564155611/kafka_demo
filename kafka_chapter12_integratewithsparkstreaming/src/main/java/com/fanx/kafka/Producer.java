package com.fanx.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        KafkaProducer<String, String> producer = KafkaUtil.buildProducer(KafkaUtil.producerConfig());
        Random random = new Random();
        while (true) {
            String msg = String.valueOf(random.nextInt(10));
            ProducerRecord<String, String> message = KafkaUtil.producerRecord(msg);
            producer.send(message).get();
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
