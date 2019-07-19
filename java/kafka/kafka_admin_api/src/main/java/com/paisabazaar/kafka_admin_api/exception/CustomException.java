package com.paisabazaar.kafka_admin_api.exception;

import java.util.List;
import java.util.StringJoiner;

/**
 * Contributed By: Tushar Mudgal
 * On: 19/7/19 | 1:21 PM
 */
public class CustomException extends Exception {
    private String message;

    public CustomException(String message) {
        super(message);
        this.message = message;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CustomException.class.getSimpleName() + "[", "]")
                .add("message='" + message + "'")
                .toString();
    }

    @Override
    public String getMessage() {
        return this.message;
    }

    @Override
    public synchronized Throwable getCause() {
        return super.getCause();
    }
}
