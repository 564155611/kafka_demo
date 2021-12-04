package com.fanx.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

public class ProducerClient {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer =
                KafkaUtil.buildProducer(KafkaUtil.producerConfig());
        ProducerRecord<String, String> record1 =
                KafkaUtil.producerRecord(null, "msg_ttl_1",
                        new LinkedHashMap<String, Object>() {{
                            put("timestamp", System.currentTimeMillis());
                            put("ttl", 20);
                        }});
        ProducerRecord<String, String> record2 =
                KafkaUtil.producerRecord(null, "msg_ttl_2",
                        new LinkedHashMap<String, Object>() {{
                            put("timestamp", System.currentTimeMillis()-5*1000);
                            put("ttl", 5);
                        }});
        ProducerRecord<String, String> record3 =
                KafkaUtil.producerRecord(null, "msg_ttl_3",
                        new LinkedHashMap<String, Object>() {{
                            put("timestamp", System.currentTimeMillis());
                            put("ttl", 30);
                        }});

        producer.send(record1).get();
        producer.send(record2).get();
        producer.send(record3).get();
    }
}
