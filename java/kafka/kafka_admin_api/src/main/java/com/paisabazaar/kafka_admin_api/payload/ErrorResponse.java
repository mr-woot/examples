package com.paisabazaar.kafka_admin_api.payload;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatus;

import java.util.Map;
/**
 * Contributed By: Tushar Mudgal
 * On: 19/7/19 | 11:37 AM
 */
@Getter
@Setter
@AllArgsConstructor
public class ErrorResponse {
    private int status;
    private String error;
}
