package com.kafka.admin;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;

public class KafkaAdminUtil
{
    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        /**=====================Create Topics======================**/
        int partitions = 10;
        short replicationFactors = 3;
        String topicName = "test-event-topic";
        createTopics(partitions, replicationFactors, topicName);

        /**=====================Describe Topics======================**/
        describeTopic(Arrays.asList("test-event-topic"));
    }

    /**
     * Create topics
     * <p>
     * Command : >kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic test-event
     * @param partitions
     * @param replicationFactors
     * @param topicName
     */
    public static void createTopics(int partitions, short replicationFactors, String topicName)
    {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(config);

        Map<String, String> configs = new HashMap<String, String>();
        CreateTopicsResult createtopicResults = admin.createTopics(Arrays.asList(
                new NewTopic(topicName, partitions, replicationFactors).configs(configs)
                ));
        KafkaFuture<Void> all = createtopicResults.all();
        System.out.println("Completed : " + all.isDone());
    }

    /**
     * Describe topics
     * <p>
     * Command : >kafka-topics.bat --describe --zookeeper localhost:2181 --topic test-event-topic
     * @param topics
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void describeTopic(List<String> topics) throws InterruptedException, ExecutionException
    {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(config);

        DescribeTopicsResult descTopicsResult = admin.describeTopics(topics);
        KafkaFuture<Map<String, TopicDescription>> kafkaFuture = descTopicsResult.all();

        Map<String, TopicDescription> topicsMap = kafkaFuture.get();

        for (TopicDescription topicDescription : topicsMap.values())
        {
            System.out.printf("name = %s, partitions = %s", topicDescription.name(), topicDescription.partitions().toString());
        }

    }
}
