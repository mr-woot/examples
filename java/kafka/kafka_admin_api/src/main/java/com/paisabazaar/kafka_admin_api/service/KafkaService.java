package com.paisabazaar.kafka_admin_api.service;

import com.paisabazaar.kafka_admin_api.exception.CustomException;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigResource;
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
@Log4j
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

    public void createTopics(List<String> topics, int numOfPartitions, int replicationFactor) throws ExecutionException, InterruptedException, KafkaException, CustomException {
        List<NewTopic> newTopics = new ArrayList<>();
        for (String topic : topics) {
            NewTopic newTopic = new NewTopic(topic, numOfPartitions, (short) replicationFactor);
            newTopics.add(newTopic);
        }
        CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
        Map<String, KafkaFuture<Void>> futures = createTopicsResult.values();

        List<String> failures = new ArrayList<>();

        for (KafkaFuture<Void> value : futures.values()) {
            try {
                value.get();
            } catch (Exception e) {
                log.error("CreateTopicsResult#all: internal error | " + e.getMessage());
                failures.add(e.getCause().getMessage());
            }
        }
        adminClient.close();
        this.initAdminClient(kafkaAdmin);

        if (failures.size() > 0) {
            throw new CustomException(String.join(", ", failures));
        }
    }

    public void deleteTopics(List<String> topics) throws ExecutionException, InterruptedException, CustomException {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);

        Map<String, KafkaFuture<Void>> futures = deleteTopicsResult.values();

        List<String> failures = new ArrayList<>();

        for (KafkaFuture<Void> value : futures.values()) {
            try {
                value.get();
            } catch (Exception e) {
                log.error("DeleteTopicsResult#all: internal error | " + e.getMessage());
                failures.add(e.getCause().getMessage());
            }
        }
        adminClient.close();
        this.initAdminClient(kafkaAdmin);

        if (failures.size() > 0) {
            throw new CustomException(String.join(", ", failures));
        }
    }

    public void createPartitions(String topicName, int partitions) throws ExecutionException, InterruptedException {
        NewPartitions newPartitionRequest = NewPartitions.increaseTo(partitions);
        adminClient.createPartitions(Collections.singletonMap(topicName, newPartitionRequest)).all().get();
        adminClient.close();
        this.initAdminClient(kafkaAdmin);
    }

    public void createRetention(String topicName, int retention) throws ExecutionException, InterruptedException {
//        NewPartitions newPartitionRequest = NewPartitions.increaseTo(partitions);
        Map<ConfigResource, Config> config;
        adminClient.close();
        this.initAdminClient(kafkaAdmin);
    }
}
