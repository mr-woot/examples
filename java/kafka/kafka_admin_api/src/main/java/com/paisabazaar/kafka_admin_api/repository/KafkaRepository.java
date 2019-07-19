package com.paisabazaar.kafka_admin_api.repository;

import com.paisabazaar.kafka_admin_api.exception.CustomException;
import com.paisabazaar.kafka_admin_api.service.KafkaService;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
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

    public void createTopics(List<String> topics, int numOfPartitions, int replicationFactor) throws ExecutionException, InterruptedException, CustomException {
        kafkaService.createTopics(topics, numOfPartitions, replicationFactor);
    }

    public void deleteTopics(List<String> topics) throws ExecutionException, InterruptedException, CustomException {
        kafkaService.deleteTopics(topics);
    }

    public void createPartitions(String topicName, int partitions) throws ExecutionException, InterruptedException {
        kafkaService.createPartitions(topicName, partitions);
    }
}
