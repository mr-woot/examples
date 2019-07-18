package com.paisabazaar.kafka_admin_api.model;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Contributed By: Tushar Mudgal
 * On: 18/7/19 | 5:10 PM
 */
@Component
public class TopicConfig {

    private final Environment env;

    @Autowired
    public TopicConfig(Environment env) {
        this.env = env;
    }
    private int numOfPartitions;

    private int replicationFactor;


    public int getNumOfPartitions() {
        return Integer.parseInt(env.getProperty("kafka.numOfPartitions"));
    }

    public void setNumOfPartitions(int numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    public int getReplicationFactor() {
        return Integer.parseInt(env.getProperty("kafka.replicationFactor"));
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
}
