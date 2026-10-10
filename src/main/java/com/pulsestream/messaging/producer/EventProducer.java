package com.pulsestream.messaging.producer;

import com.pulsestream.domain.event.ActivityEvent;
import org.springframework.kafka.support.SendResult;

/**
 * Contract for publishing activity events to downstream message brokers.
 */
public interface EventProducer {

    /**
     * Publishes a validated domain activity event to the streaming broker.
     *
     * <p>Blocks synchronously until broker acknowledgement or until timeout expires.
     *
     * @param event Validated activity event to publish.
     * @return {@link SendResult} containing Kafka record metadata (topic, partition, offset).
     * @throws com.pulsestream.exception.EventPublishingException if publishing times out or fails.
     */
    SendResult<String, ActivityEvent> sendEvent(ActivityEvent event);
}
