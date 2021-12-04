package com.fanx.kafka;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 遍历主题然后遍历这个主题的所有分区,从订阅该主题的所有消费者中随机抽取一个作为随机消费者然后订阅这个遍历的分区
 * 如果需要在客户端中使用该策略,则:properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
 *  RandomAssignor.class.getName())
 */
public class RandomAssignor extends AbstractPartitionAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        Map<String,List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<>());
        }
        //针对每个主题进行分区分配
        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<String> consumersForTopic = topicEntry.getValue();
            int consumerSize = consumersForTopic.size();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null) {
                continue;
            }

            List<TopicPartition> partitions =
                    AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            for (TopicPartition partition : partitions) {
                int rand = new Random().nextInt(consumerSize);
                String randomConsumer = consumersForTopic.get(rand);
                assignment.get(randomConsumer).add(partition);
            }
        }
        return assignment;
    }

    @Override
    public String name() {
        return "random";
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
