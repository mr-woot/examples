package com.paisabazaar.kafka_admin_api.controller;

import com.google.gson.Gson;
import com.paisabazaar.kafka_admin_api.exception.CustomException;
import com.paisabazaar.kafka_admin_api.model.TopicConfig;
import com.paisabazaar.kafka_admin_api.payload.ErrorResponse;
import com.paisabazaar.kafka_admin_api.payload.TopicPartitionRequest;
import com.paisabazaar.kafka_admin_api.payload.TopicRequest;
import com.paisabazaar.kafka_admin_api.repository.KafkaRepository;
import lombok.extern.log4j.Log4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.Map;

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
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getCause().getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping(value = "/describe-consumer-groups", produces = "application/json")
    public ResponseEntity<?> describeConsumerGroups() {
        String result = null;
        try {
            result = new Gson().toJson(kafkaRepository.describeConsumerGroups());
        } catch (ExecutionException | InterruptedException e) {
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getCause().getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping(value = "/get-topics", produces = "application/json")
    public ResponseEntity<?> getTopics() {
        String result = null;
        try {
            result = new Gson().toJson(kafkaRepository.getTopicNames());
        } catch (ExecutionException | InterruptedException e) {
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getCause().getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping(value = "/get-consumer-groups", produces = "application/json")
    public ResponseEntity<?> getConsumerGroups() {
        String result = null;
        try {
            result = new Gson().toJson(kafkaRepository.getConsumerGroups());
        } catch (ExecutionException | InterruptedException e) {
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getCause().getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping(value = "/create-topics", produces = "application/json")
    public ResponseEntity<?> createTopics(@RequestBody TopicRequest topicRequest) {
        try {
            kafkaRepository.createTopics(topicRequest.getTopics(), topicConfig.getNumOfPartitions(), topicConfig.getReplicationFactor());
        } catch (ExecutionException | InterruptedException e) {
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getCause().getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (CustomException e) {
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.CONFLICT.value(), e.getMessage()), HttpStatus.CONFLICT);
        }
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @PostMapping(value = "/delete-topics", produces = "application/json")
    public ResponseEntity<?> deleteTopics(@RequestBody TopicRequest topicRequest) {
        try {
            kafkaRepository.deleteTopics(topicRequest.getTopics());
        } catch (ExecutionException | InterruptedException e) {
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getCause().getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (CustomException e) {
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.CONFLICT.value(), e.getMessage()), HttpStatus.CONFLICT);
        }
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @PostMapping(value = "/create-partitions", produces = "application/json")
    public ResponseEntity<?> createPartitions(@RequestBody HashMap<String, Integer> topicPartitionRequest) {
        try {
            for (Map.Entry<String, Integer> entry : topicPartitionRequest.entrySet()) {
                System.out.println(entry.getKey() + entry.getValue());
                kafkaRepository.createPartitions(entry.getKey(), entry.getValue());
            }
        } catch (ExecutionException | InterruptedException e) {
            return new ResponseEntity<>(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getCause().getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(HttpStatus.CREATED);
    }


}
