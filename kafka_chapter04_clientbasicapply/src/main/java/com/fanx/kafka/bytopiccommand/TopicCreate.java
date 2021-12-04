package com.fanx.kafka.bytopiccommand;

public class TopicCreate {
    public static void createTopic(){
        String[] options = new String[]{
                "--zookeeper", "localhost:2181/kafka",
                "--create",
                "--replication-factor", "1",
                "--partitions", "1",
                "--topic", "topic-create-api"
        };
        kafka.admin.TopicCommand.main(options);
    }

    public static void main(String[] args) {
        System.out.println(1);
    }
}
