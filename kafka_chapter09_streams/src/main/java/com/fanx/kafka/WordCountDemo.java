package com.fanx.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        /*构建kafka streams的配置
         * application.id(APPLICATION_ID_CONFIG):
         *   每个kafka streams应用程序都必须有一个application.id,这个applicationId用于协调应用实例,
         *   也用于命名内部的本地存储和相关主题.整个kafka集群中,applicationId必须唯一.
         * bootstrap.servers:kafka集群的地址
         * default.key.serde和default.value.serde:分别用来设置key和value的序列化器
         * */
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        /*创建一个KStreamBuilder实例*/
        StreamsBuilder builder = new StreamsBuilder();
        /*设置builder的KStream实例,并设定数据源头是streams-plaintext-input主题.
         * 然后设置builder的KTable实例:其中设置流式处理逻辑
         * KStream:
         *   是一个键值组成的抽象记录流,每个键值对是一个独立的单元,即使相同的key也不会覆盖,类似数据库的插入操作
         * KTable:
         *   一个基于表主键的日志更新流,相同key的每条记录只保存最新的一条记录,类似于数据库基于主键的更新操作
         * 无论是KStream记录流还是KTable更新日志流,都可以从一个或多个Kafka主题数据源来创建.
         * KStream与KTable相互转换和操作:
         *   一个KStream可以与另一个KStream或KTable进行join操作成为新的KStream,或者聚合成为新的KTable
         *   KStream和KTable本身也可以相互转换.
         * */
        KStream<String, String> source =
                builder.stream("streams-plaintext-input");
        KTable<String, Long> counts = source
                .flatMapValues(
                        value -> Arrays.asList(value
                                .toLowerCase(Locale.getDefault())
                                .split(" ")))
                .groupBy((key, value) -> value)
                .count();
        //以流式输出的方式将结果输出到streams-wordcount-output主题中
        counts.toStream().to("streams-wordcount-output",
                Produced.with(Serdes.String(), Serdes.Long()));

        /*
         * 基于上面KStream和KTable的逻辑和props配置创建一个KafkaStreams对象,
         * 并启动KafkaStreams引擎
         * */
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }, "streams-wordcount-shutdown-hook"));

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
