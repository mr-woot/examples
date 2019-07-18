package com.paisabazaar.kafka_admin_api.repository;

import com.paisabazaar.kafka_admin_api.service.KafkaService;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Contributed By: Tushar Mudgal
 * On: 18/7/19 | 1:05 PM
 */
@Repository
public class KafkaRepository {

    private final KafkaService kafkaService;

    @Autowired
    public KafkaRepository(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    public Map<String, TopicDescription> describeTopics() throws ExecutionException, InterruptedException {
        return kafkaService.describeTopics();
    }

    public Set<String> getTopicNames() throws ExecutionException, InterruptedException {
        return kafkaService.getTopicNames();
    }

    public Map<String, ConsumerGroupDescription> describeConsumerGroups() throws ExecutionException, InterruptedException {
        return kafkaService.describeConsumerGroups();
    }

    public List getConsumerGroups() throws ExecutionException, InterruptedException {
        return kafkaService.getConsumerGroups();
    }

    public void createTopic(String topicName, int numOfPartitions, int replicationFactor) throws ExecutionException, InterruptedException {
        kafkaService.createTopic(topicName, numOfPartitions, replicationFactor);
    }

    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        kafkaService.deleteTopic(topicName);
    }
}
