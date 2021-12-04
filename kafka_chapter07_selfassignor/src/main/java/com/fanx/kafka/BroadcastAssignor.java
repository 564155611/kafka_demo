package com.fanx.kafka;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 让所有消费者都分配到自己订阅的主题下的所有分区(问题:__consumer_offsets的覆盖问题)
 */
public class BroadcastAssignor extends AbstractPartitionAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String topic : partitionsPerTopic.keySet()) {
            Integer numPartitions = partitionsPerTopic.get(topic);
            List<String> consumers = consumersPerTopic.get(topic);
            if (numPartitions == null || consumers.isEmpty()) {
                continue;
            }
            List<TopicPartition> partitions =
                    AbstractPartitionAssignor.partitions(topic, numPartitions);
            if (!partitions.isEmpty()) {
                for (String consumer : consumers) {
                    assignment.put(consumer, partitions);
                }
            }
        }
        return assignment;
    }

    @Override
    public String name() {
        return "broadcast";
    }

    private Map<String, List<String>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue().topics())
                put(res, topic, consumerId);
        }
        return res;
    }
}
