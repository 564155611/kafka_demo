package com.fanx.kafka.monitorbyjmx.getlag.javacode;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaConsumerGroupService2 {
    private String brokerList;
    private AdminClient adminClient;
    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerGroupService2(String brokerList) {
        this.brokerList = brokerList;
    }

    public void init() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        adminClient = AdminClient.create(props);
        kafkaConsumer = ConsumerGroupUtils.createNewConsumer(brokerList,
                "kafkaAdminClientDemoGroupId");
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }

    public List<PartitionAssignmentState> getResult(String group) throws ExecutionException, InterruptedException {
        List<PartitionAssignmentState> result = new ArrayList<>();
        if (group == null) {
            return result;
        }

        Map<String, ConsumerGroupDescription> groupResult =
                adminClient.describeConsumerGroups(Collections.singleton(group)).all().get();
        final Map<TopicPartition, OffsetAndMetadata> groupPartitionOffsetResult =
                adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        if (groupResult.isEmpty()) {
            return result;
        }
        Set<TopicPartition> assignedTps = new HashSet<>();
        ConsumerGroupDescription description = groupResult.get(group);
        System.out.println("消费组当前状态为: " + description.state());
        final Collection<MemberDescription> members = description.members();
        Map<TopicPartition, MemberDescription> indexMembers = new HashMap<>();
        for (MemberDescription member : members) {
            final MemberAssignment assignment = member.assignment();
            if (assignment == null) {
                continue;
            }
            final Set<TopicPartition> topicPartitions = assignment.topicPartitions();
            if (topicPartitions.isEmpty()) {
                result.add(PartitionAssignmentState.builder()
                        .group(group).coordinator(description.coordinator())
                        .consumerId(member.consumerId()).host(member.host())
                        .clientId(member.clientId()).build());

            } else {
                assignedTps.addAll(topicPartitions);
                indexMembers.putAll(topicPartitions.stream().collect(Collectors.toMap(k -> k, v -> member)));
            }
        }
        final Set<TopicPartition> offsetsKeySet = groupPartitionOffsetResult.keySet();
        final HashSet<TopicPartition> allTps = new HashSet<>(offsetsKeySet);
        allTps.addAll(assignedTps);
        final Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(allTps);
        final Set<TopicPartition> onlyAssignKeySet = assignedTps.stream()
                .filter(assignTp -> !offsetsKeySet.contains(assignTp))
                .collect(Collectors.toSet());
        if (!onlyAssignKeySet.isEmpty()) {
            /*这对应某些消费者刚刚启动时,尚未提交自己的offset到__consumer_offsets主题中*/
            final Map<TopicPartition, Long> beginningOffsets =
                    kafkaConsumer.beginningOffsets(onlyAssignKeySet);
            for (TopicPartition tp : onlyAssignKeySet) {
                final MemberDescription md = indexMembers.get(tp);
                final Long logSize = endOffsets.get(tp);
                final Long offset = beginningOffsets.get(tp);
                final long lag = lag(offset, logSize);
                result.add(PartitionAssignmentState.builder()
                        .group(group).coordinator(description.coordinator())
                        .logSize(logSize).offset(offset).lag(lag)
                        .topic(tp.topic()).partition((tp.partition()))
                        .host(md.host()).consumerId(md.consumerId())
                        .clientId(md.clientId()).build());
            }
        }

        for (TopicPartition tp : offsetsKeySet) {
            final long offset = groupPartitionOffsetResult.get(tp).offset();
            final Long endOffset = endOffsets.get(tp);
            final long lag = lag(offset, endOffset);
            if (assignedTps.contains(tp)) {
                /*这对应某些消费者处于活跃状态正在消费的情况*/
                final MemberDescription md = indexMembers.get(tp);
                result.add(PartitionAssignmentState.builder()
                        .group(group).coordinator(description.coordinator())
                        .logSize(endOffsets.get(tp)).topic(tp.topic())
                        .partition((tp.partition())).host(md.host())
                        .consumerId(md.consumerId()).clientId(md.clientId())
                        .offset(offset).lag(lag)
                        .build());

            } else {
                /*这对应__consumer_offsets中有对应的消费位移,但是消费者却不处于活跃状态,
                如果所有的消费者都处于这个状态,那么上面description字段的状态state值为Empty*/
                result.add(PartitionAssignmentState.builder()
                        .group(group).coordinator(description.coordinator())
                        .logSize(endOffset).topic(tp.topic())
                        .partition((tp.partition())).offset(offset)
                        .lag(lag).build());
            }
        }

        return result;
    }

    private long lag(long offset, long logSize) {
        long lag = logSize - offset;
        return lag < 0 ? 0 : lag;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaConsumerGroupService2 service =
                new KafkaConsumerGroupService2("localhost:9092");
        service.init();
        List<PartitionAssignmentState> list =
                service.getResult("groupIdMonitor");
        ConsumerGroupUtils.printPasList(list);
        service.close();
    }
}
