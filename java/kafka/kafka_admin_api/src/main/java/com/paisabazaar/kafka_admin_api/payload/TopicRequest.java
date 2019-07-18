package com.paisabazaar.kafka_admin_api.payload;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Contributed By: Tushar Mudgal
 * On: 18/7/19 | 4:29 PM
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TopicRequest {
    private String topicName;

}
