package com.fanx.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class TheOldRebalanceListener implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            // 提交消费位移
            //commitOffsets(partition);
            // 清除消费状态
            //cleanupState(partition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            // 初始化消费状态
            //initializeState(partition);
            // 初始化消费位移
            //initializeOffset(partition);
        }
    }
}
