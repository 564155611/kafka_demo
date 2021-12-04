package com.fanx.kafka.monitorbyjmx.getlag.javacode;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class ConsumerGroupUtils {
    public static KafkaConsumer<String, String> createNewConsumer(String brokerList, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, "consumerClient");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<String, String>(properties);
    }

    public static void printPasList(List<PartitionAssignmentState> list) {
        System.out.println(String.format(
                "%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                "TOPIC", "PARTITION",
                "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG",
                "CONSUMER-ID", "HOST", "CLIENT-ID"));

        list.forEach(item->{
            System.out.println(String.format(
                    "%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                    item.getTopic(), item.getPartition(),item.getOffset(),
                    item.getLogSize(), item.getLag(),
                    Optional.ofNullable(item.getConsumerId()).orElse("-"),
                    Optional.ofNullable(item.getHost()).orElse("-"),
                    Optional.ofNullable(item.getClientId()).orElse("-")));
        });
    }
}
