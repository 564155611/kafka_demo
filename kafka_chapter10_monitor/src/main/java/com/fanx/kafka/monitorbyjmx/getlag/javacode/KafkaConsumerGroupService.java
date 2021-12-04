package com.fanx.kafka.monitorbyjmx.getlag.javacode;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

@Slf4j
public class KafkaConsumerGroupService {
    private String brokerList;
    private AdminClient adminClient;
    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerGroupService(String brokerList) {
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

    public List<PartitionAssignmentState> collectGroupAssignment(String group) throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult groupsResult =
                adminClient.describeConsumerGroups(Collections.singleton(group));
        ConsumerGroupDescription description =
                groupsResult.all().get().get(group);
        List<TopicPartition> assignedTps = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        Collection<MemberDescription> members = description.members();
        if (members != null) {
            ListConsumerGroupOffsetsResult offsetsResult =
                    adminClient.listConsumerGroupOffsets(group);
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    offsetsResult.partitionsToOffsetAndMetadata().get();
            if (offsets != null && !offsets.isEmpty()) {
                String state = description.state().toString();
                if (state.equals("Stable")) {
                    rowsWithConsumer = getRowsWithConsumer(description, offsets,
                            members, assignedTps, group);
                }
            }
            List<PartitionAssignmentState> rowsWithoutConsumer =
                    getRowsWithoutConsumer(description, offsets, assignedTps, group);
            rowsWithConsumer.addAll(rowsWithoutConsumer);
        }
        return rowsWithConsumer;
    }


    private List<PartitionAssignmentState> getRowsWithConsumer(
            ConsumerGroupDescription description,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            Collection<MemberDescription> members,
            List<TopicPartition> assignedTps,
            String group) {
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        for (MemberDescription member : members) {
            MemberAssignment assignment = member.assignment();
            if (assignment == null) {
                continue;
            }
            Set<TopicPartition> tpSet = assignment.topicPartitions();
            if (tpSet.isEmpty()) {
                rowsWithConsumer.add(PartitionAssignmentState.builder()
                        .group(group).coordinator(description.coordinator())
                        .consumerId(member.consumerId()).host(member.host())
                        .clientId(member.clientId()).build());
            } else {
                Map<TopicPartition, Long> logSizes =
                        kafkaConsumer.endOffsets(tpSet);
                assignedTps.addAll(tpSet);
                List<PartitionAssignmentState> tempList = tpSet.stream()
                        .sorted(comparing(TopicPartition::partition))
                        .map(tp -> getPasWithConsumer(logSizes, offsets, tp, group, member, description))
                        .collect(Collectors.toList());
                rowsWithConsumer.addAll(tempList);
            }
        }
        return rowsWithConsumer;
    }

    private PartitionAssignmentState getPasWithConsumer(Map<TopicPartition, Long> logSizes,
                                                        Map<TopicPartition, OffsetAndMetadata> offsets,
                                                        TopicPartition tp, String group,
                                                        MemberDescription member,
                                                        ConsumerGroupDescription description) {
        long logSize = logSizes.get(tp);
        if (offsets.containsKey(tp)) {
            long offset = offsets.get(tp).offset();
            long lag = getLag(offset, logSize);
            return PartitionAssignmentState.builder().group(group)
                    .coordinator(description.coordinator()).lag(lag)
                    .topic(tp.topic()).partition(tp.partition())
                    .offset(offset).consumerId(member.consumerId())
                    .host(member.host()).clientId(member.clientId())
                    .logSize(logSize).build();
        }
        return PartitionAssignmentState.builder()
                .group(group).coordinator(description.coordinator())
                .topic(tp.topic()).partition(tp.partition())
                .consumerId(member.consumerId()).host(member.host())
                .clientId(member.clientId()).logSize(logSize).build();
    }

    private long getLag(long offset, long logSize) {
        long lag = logSize - offset;
        return lag < 0 ? 0 : lag;
    }

    private List<PartitionAssignmentState> getRowsWithoutConsumer(
            ConsumerGroupDescription description,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            List<TopicPartition> assignedTps, String group) {
        Set<TopicPartition> tpSet = offsets.keySet();
        return tpSet.stream().filter(tp -> !assignedTps.contains(tp))
                .map(tp->{
                    long logSize = 0;
                    Long endOffset = kafkaConsumer
                            .endOffsets(Collections.singleton(tp)).get(tp);
                    if (endOffset!=null) {
                        logSize = endOffset;
                    }
                    long offset = offsets.get(tp).offset();
                    return PartitionAssignmentState.builder().group(group)
                            .coordinator(description.coordinator())
                            .topic(tp.topic()).partition(tp.partition())
                            .logSize(logSize).lag(getLag(offset, logSize))
                            .offset(offset).build();
                }).sorted(comparing(PartitionAssignmentState::getPartition))
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaConsumerGroupService service =
                new KafkaConsumerGroupService("localhost:9092");
        service.init();
        List<PartitionAssignmentState> list =
                service.collectGroupAssignment("groupIdMonitor");
        ConsumerGroupUtils.printPasList(list);
        service.close();
    }
}
