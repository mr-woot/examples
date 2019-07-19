package com.paisabazaar.kafka_admin_api.payload;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;

/**
 * Contributed By: Tushar Mudgal
 * On: 18/7/19 | 4:29 PM
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TopicPartitionRequest {
    private HashMap<String, Integer> map;
}
