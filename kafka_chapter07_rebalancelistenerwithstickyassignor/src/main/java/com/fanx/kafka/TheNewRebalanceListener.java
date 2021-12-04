package com.fanx.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

public class TheNewRebalanceListener implements ConsumerRebalanceListener {
    Collection<TopicPartition> lastAssignment = Collections.emptyList();
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            // 提交消费位移
            //commitOffsets(partition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignment) {
        for (TopicPartition partition : subtract(lastAssignment,assignment)) {
            //对再均衡后从当前消费者移出去的分区做清理消费状态操作
            //cleanupState(partition);
        }
        for (TopicPartition partition : subtract(assignment,lastAssignment)) {
            //处理再均衡后加入当前消费者的分区做初始化消费状态操作
            //initializeState(partition);
        }
        for (TopicPartition partition : assignment) {
            // 对当前拥有的所有分区初始化消费位移
            //initializeOffset(partition);
        }
        this.lastAssignment = assignment;

    }

    private Iterable<? extends TopicPartition> subtract(Collection<TopicPartition> assignment1, Collection<TopicPartition> assignment2) {
        HashSet<TopicPartition> topicPartitions1 = new HashSet<>(assignment1);
        topicPartitions1.removeAll(assignment2);
        return topicPartitions1;
    }
}
