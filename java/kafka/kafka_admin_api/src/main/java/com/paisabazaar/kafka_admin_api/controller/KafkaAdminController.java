package com.paisabazaar.kafka_admin_api.controller;

import com.google.gson.Gson;
import com.paisabazaar.kafka_admin_api.model.TopicConfig;
import com.paisabazaar.kafka_admin_api.payload.TopicRequest;
import com.paisabazaar.kafka_admin_api.repository.KafkaRepository;
import lombok.extern.log4j.Log4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

/**
 * Contributed By: Tushar Mudgal
 * On: 17/7/19 | 4:50 PM
 */
@RestController
@RequestMapping("/api")
@Log4j
public class KafkaAdminController {

    private final TopicConfig topicConfig;

    private final KafkaRepository kafkaRepository;

    @Autowired
    public KafkaAdminController(KafkaRepository kafkaRepository, TopicConfig topicConfig) {
        this.kafkaRepository = kafkaRepository;
        this.topicConfig = topicConfig;
    }

    @GetMapping(value = "/describe-topics", produces = "application/json")
    public ResponseEntity<?> describeTopics() {
        String result = null;
        try {
            result = new Gson().toJson(kafkaRepository.describeTopics());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping(value = "/describe-consumer-groups", produces = "application/json")
    public ResponseEntity<?> describeConsumerGroups() {
        String result = null;
        try {
            result = new Gson().toJson(kafkaRepository.describeConsumerGroups());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping(value = "/get-topics", produces = "application/json")
    public ResponseEntity<?> getTopics() {
        String result = null;
        try {
            result = new Gson().toJson(kafkaRepository.getTopicNames());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping(value = "/get-consumer-groups", produces = "application/json")
    public ResponseEntity<?> getConsumerGroups() {
        String result = null;
        try {
            result = new Gson().toJson(kafkaRepository.getConsumerGroups());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping(value = "/create-topic", produces = "application/json")
    public ResponseEntity<?> createTopic(@RequestBody TopicRequest topicRequest) {
        try {
            kafkaRepository.createTopic(topicRequest.getTopicName(), topicConfig.getNumOfPartitions(), topicConfig.getReplicationFactor());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @PostMapping(value = "/delete-topic", produces = "application/json")
    public ResponseEntity<?> deleteTopic(@RequestBody TopicRequest topicRequest) {
        try {
            kafkaRepository.deleteTopic(topicRequest.getTopicName());
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

}
