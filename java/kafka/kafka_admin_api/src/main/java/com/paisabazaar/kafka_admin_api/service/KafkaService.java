package com.paisabazaar.kafka_admin_api.service;

import org.apache.kafka.clients.admin.*;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Contributed By: Tushar Mudgal
 * On: 18/7/19 | 1:07 PM
 */
@Service
public class KafkaService {
    private final KafkaAdmin kafkaAdmin;
    private AdminClient adminClient;

    public KafkaService(AdminClient adminClient, KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
        this.adminClient = AdminClient.create(kafkaAdmin.getConfig());
    }

    private void initAdminClient(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfig());
    }

    public Set<String> getTopicNames() throws ExecutionException, InterruptedException {
        return adminClient.listTopics().names().get();
    }

    public Map<String, TopicDescription> describeTopics() throws ExecutionException, InterruptedException {
        Set<String> topicNames = getTopicNames();
        return adminClient.describeTopics(topicNames).all().get();
    }

    public Map<String, ConsumerGroupDescription> describeConsumerGroups() throws ExecutionException, InterruptedException {
        //Here you get all the consumer groups
        List<String> groupIds;
        groupIds = adminClient.listConsumerGroups().all().get().
                    stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());

        // Here you get all the descriptions for the groups
        Map<String, ConsumerGroupDescription> groups;
        groups = adminClient.describeConsumerGroups(groupIds).all().get();
        return groups;
    }

    public List getConsumerGroups() throws ExecutionException, InterruptedException {
        return adminClient.listConsumerGroups().all().get().
                stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
    }

    public void createTopic(String topicName, int numOfPartitions, int replicationFactor) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, numOfPartitions, (short) replicationFactor); //new NewTopic(topicName, numPartitions, replicationFactor)

        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(newTopic);

        adminClient.createTopics(newTopics).all().get();
        adminClient.close();

        this.initAdminClient(kafkaAdmin);
    }

    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        Collection<String> topics = new ArrayList<>();
        topics.add(topicName);
        adminClient.deleteTopics(topics).all().get();
        adminClient.close();

        this.initAdminClient(kafkaAdmin);
    }
}
