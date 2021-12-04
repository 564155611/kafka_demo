package com.fanx.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 */
@Slf4j
public class KafkaConsumerAnalysis {
    public static final String BROKER_LIST = "192.168.12.171:9092,192.168.12.172:9092,192.168.12.173:9092";
    public static final String TOPIC = "topic-demo";
    public static final String GROUP_ID = "group.demo";
    public static final String CLIENT_ID = "consumer.client.id.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = buildConsumer1(properties);
        subscribe1(consumer);

        try {
            while (isRunning.get()) {
                consume1(consumer);
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            consumer.close();
        }
    }

    public static void consume1(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("topic = " + record.topic() +
                    ",partition = " + record.partition() +
                    ",offset = " + record.offset());
            System.out.println("key = " + record.key() +
                    ", value = " + record.value());
        }
    }

    /**
     * 按照分区分组消费
     *
     * @param consumer
     */
    public static void consume2(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (TopicPartition partition : records.partitions()) {
            for (ConsumerRecord<String, String> record : records.records(partition)) {
                System.out.println("topic = " + record.topic() +
                        ",partition = " + record.partition() +
                        ",offset = " + record.offset());
                System.out.println("key = " + record.key() +
                        ", value = " + record.value());
            }
        }
    }

    /**
     * 按照主题分组消费
     *
     * @param consumer
     */
    public static void consume3(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (String topic : Arrays.asList("topic-demo1", "topic-demo2")) {
            for (ConsumerRecord<String, String> record : records.records(topic)) {
                System.out.println("topic = " + record.topic() +
                        ",partition = " + record.partition() +
                        ",offset = " + record.offset());
                System.out.println("key = " + record.key() +
                        ", value = " + record.value());
            }
        }
    }

    /**
     * 三种offset:lastConsumedOffset,position,committedOffset
     *
     * @param consumer
     */
    public static void consume4(KafkaConsumer<String, String> consumer) {
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        consumer.assign(Arrays.asList(tp));
        long lastConsumedOffset = -1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                break;
            }
            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
            lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            consumer.commitSync();
        }

        System.out.println("consumed offset is " + lastConsumedOffset);
        OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
        System.out.println("committed offset is " + offsetAndMetadata.offset());
        long position = consumer.position(tp);
        System.out.println("the offset of the next record is " + position);
    }

    /**
     * 同步提交
     *
     * @param consumer
     */
    public static void consume5(KafkaConsumer<String, String> consumer) {
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                //do some logical processing
            }
            consumer.commitSync();
        }
    }

    /**
     * 批量同步提交
     *
     * @param consumer
     */
    public static void consume6(KafkaConsumer<String, String> consumer) {
        final int minBatchSize = 200;
        List<ConsumerRecord> buffer = new ArrayList<>();
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //do some logical processing with buffer.
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    /**
     * 带参数的同步提交:
     * 消息级别的细粒度:每消费一条提交一条(性能很差)
     *
     * @param consumer
     */
    public static void consume7(KafkaConsumer<String, String> consumer) {
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                //do some logical processing
                long offset = record.offset();
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
            }
        }
    }

    /**
     * 带参数的同步提交:
     * 分区级别的细粒度:按照各个分区的lastConsumedOffset+1提交
     *
     * @param consumer
     */
    public static void consume8(KafkaConsumer<String, String> consumer) {
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        //do some logical processing
                    }
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumedOffset + 1)));
                }
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing
                    long offset = record.offset();
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 异步提交+回调
     */
    public static void consume9(KafkaConsumer<String, String> consumer) {
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                //do some logical processing
            }
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception == null) {
                        System.out.println(offsets);
                    } else {
                        log.error("fail to commit offsets {}", offsets,exception);
                    }
                }
            });
        }
    }

    /**
     * 异步提交+同步提交作为退出时的把关
     */
    public static void consume10(KafkaConsumer<String, String> consumer) {
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing
                }
                consumer.commitAsync();
            }
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * seek方法
     * @param consumer
     */
    public static void consume11(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList(TOPIC));
        consumer.poll(Duration.ofMillis(10000));
        Set<TopicPartition> assignment = consumer.assignment();
        for (TopicPartition partition : assignment) {
            consumer.seek(partition, 10);
        }
        while (isRunning.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
        }
    }

    /**
     * assignment方法判定是否获得分区+seek方法
     * @param consumer
     */
    public static void consume12(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList(TOPIC));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        for (TopicPartition partition : assignment) {
            consumer.seek(partition, 10);
        }
        while (isRunning.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
        }
    }


    /**
     * assignment方法查询分区信息,然后seek方法从分区末尾开始消费
     * @param consumer
     */
    public static void consume13(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList(TOPIC));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition partition : assignment) {
            consumer.seek(partition, offsets.get(partition));
        }
        while (isRunning.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
        }
    }

    /**
     * offsetForTimes查询昨天的这个时刻的offset,然后根据这个offset调用seek方法回溯消费
     * @param consumer
     */
    public static void consume14(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList(TOPIC));

        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition partition : assignment) {
            timestampsToSearch.put(partition, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }

        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampsToSearch);
        for (TopicPartition partition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(partition);
            if (offsetAndTimestamp != null) {
                consumer.seek(partition, offsetAndTimestamp.offset());
            }
        }
        while (isRunning.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
        }
    }

    /**
     * 模拟越界:endOffset+1
     * 相关日志:
     *   Fetch offset 101 is out of range for partition topic-demo-1
     *   Resetting offset for partition topic-demo-1 to offset 100
     * @param consumer
     */
    public static void consume15(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList(TOPIC));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition partition : assignment) {
            consumer.seek(partition, offsets.get(partition)+1);
        }
        while (isRunning.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
        }
    }

    /**
     * 将各个分片的消费offset存储到DB中,消费的时候直接从DB中获取对应的offset,
     * 然后通过seek方法从DB中指定的offset处开始消费.
     * @param consumer
     */
    public static void consume16(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList(TOPIC));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        for (TopicPartition partition : assignment) {
            consumer.seek(partition, getOffsetFromDB(partition));
        }
        while (isRunning.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    //process the record
                }
                long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                //将消息位移存储在DB中
                storeOffsetToDB(partition, lastConsumedOffset + 1);
            }
        }
    }

    private static void storeOffsetToDB(TopicPartition partition, long offset) {
    }

    private static long getOffsetFromDB(TopicPartition partition) {
        return 0;
    }

    /**
     * 处理完一批消息后异步提交消费位移.
     * 在发生正常关闭或者再均衡时,同步提交消费位移(ConsumerRebalanceListener).
     * @param consumer
     */
    public static void consume17(KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener(){
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing
                    currentOffsets.put(new TopicPartition(record.topic(),record.partition()),
                            new OffsetAndMetadata(record.offset()+1));
                }
                consumer.commitAsync(currentOffsets,null);
            }
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * 在均衡之前消费者完成消费之后将offset保存到数据库中
     * 在均衡之后消费者均衡到新的分区后,根据数据库中的offset进行消费.
     * @param consumer
     */
    public static void consume18(KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener(){
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //store offsets in DB
                for (TopicPartition partition : partitions) {
                    storeOffsetToDB(partition,currentOffsets.get(partition).offset());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, getOffsetFromDB(partition));
                }
            }
        });
        //todo 消费过程
    }


    public static void subscribe1(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Collections.singletonList(TOPIC));
    }

    public static void subscribe2(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Pattern.compile("^topic-.*"));
    }

    public static void subscribe3(KafkaConsumer<String, String> consumer) {
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0)));
    }

    public static void subscribe4(KafkaConsumer<String, String> consumer) {
        List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);
        if (partitionInfos != null) {
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        consumer.assign(partitions);
    }


    public static KafkaConsumer<String, String> buildConsumer1(Properties properties) {
        return new KafkaConsumer<String, String>(properties);
    }

    public static KafkaConsumer<String, String> buildConsumer2(Properties properties) {
        return new KafkaConsumer<String, String>(properties, new StringDeserializer(), new StringDeserializer());
    }

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, CLIENT_ID);
        return properties;
    }

    public static Map<TopicPartition,Long> beginningOffsets(KafkaConsumer<String,String> kafkaConsumer){
        List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(TOPIC);
        List<TopicPartition> tpList = partitions.stream()
                .map(pInfo -> new TopicPartition(pInfo.topic(), pInfo.partition()))
                .collect(Collectors.toList());
        Map<TopicPartition, Long> beginningOffsets = kafkaConsumer.beginningOffsets(tpList);
        System.out.println(beginningOffsets);
        return beginningOffsets;
    }
}
