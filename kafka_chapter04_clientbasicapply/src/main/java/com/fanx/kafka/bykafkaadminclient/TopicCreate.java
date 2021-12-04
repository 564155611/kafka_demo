package com.fanx.kafka.bykafkaadminclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicCreate {
    public static final String BROKER_LIST = "192.168.12.171:9092,192.168.12.172:9092,192.168.12.173:9092";
    public static final String TOPIC = "topic-admin";

    public static void main(String[] args) {
        createTopic();
    }

    public static void createTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        NewTopic newTopic = buildNewTopic1();
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        client.close();

    }

    public static NewTopic buildNewTopic1() {
        return new NewTopic(TOPIC, 4, (short) 1);
    }

    /**
     * 手动指定副本分配方案
     *
     * @return
     */
    public static NewTopic buildNewTopic2() {
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, Arrays.asList(0));
        replicasAssignments.put(1, Arrays.asList(0));
        replicasAssignments.put(2, Arrays.asList(0));
        replicasAssignments.put(3, Arrays.asList(0));
        return new NewTopic(TOPIC, replicasAssignments);
    }

    /**
     * 配置主题参数
     *
     * @return
     */
    public static NewTopic buildNewTopic3() {
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, Arrays.asList(0));
        replicasAssignments.put(1, Arrays.asList(0));
        replicasAssignments.put(2, Arrays.asList(0));
        replicasAssignments.put(3, Arrays.asList(0));
        NewTopic newTopic = new NewTopic(TOPIC, replicasAssignments);
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        newTopic.configs(configs);
        return newTopic;
    }

}
