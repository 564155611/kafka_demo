package com.fanx.kafka;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

public class PolicyDemo implements CreateTopicPolicy {
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        Integer partitions = requestMetadata.numPartitions();
        Short replicationFactor = requestMetadata.replicationFactor();
        if (partitions != null || replicationFactor != null) {
            if (partitions < 5) {
                throw new PolicyViolationException("Topic should have at least 5 partitions,received: " + partitions);
            }
            if (replicationFactor <= 1) {
                throw new PolicyViolationException("Topic should have at least 2 replication factor,received: " + replicationFactor);
            }
        }
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
