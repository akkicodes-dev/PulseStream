package com.pulsestream.exception;

/**
 * Thrown when publishing an event to Apache Kafka fails, times out, or cannot be acknowledged.
 */
public class EventPublishingException extends RuntimeException {

    public EventPublishingException(String message) {
        super(message);
    }

    public EventPublishingException(String message, Throwable cause) {
        super(message, cause);
    }
}
