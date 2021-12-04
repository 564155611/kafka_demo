package com.fanx.kafka.monitorbyjmx.getlag.javacode;

import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.Node;

@Data
@Builder
public class PartitionAssignmentState {
    private String topic;
    private int partition;
    private String group;
    private Node coordinator;
    private String clientId;
    private String consumerId;
    private String host;
    private long lag;
    private long offset;
    private long logSize;
}
