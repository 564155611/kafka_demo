package com.fanx.kafka.monitorbyjmx.getlag.byscala;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PartitionAssignmentStateAnother {
    private String group;
    private Node coordinator;
    private String topic;
    private int partition;
    private long offset;
    private long lag;
    private String consumerId;
    private String host;
    private String clientId;
    private long logEndOffset;

    @Data
    public static class Node{
        public int id;
        public String idString;
        public String host;
        public int port;
        public String rack;

    }
}
