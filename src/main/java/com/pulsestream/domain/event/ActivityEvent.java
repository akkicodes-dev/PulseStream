package com.pulsestream.domain.event;

import java.time.Instant;
import java.util.Map;

/**
 * Domain model representing a validated user activity event.
 *
 * <p>This domain event is serialized to JSON and published to Kafka topic {@code user.activity.events}.
 * It encapsulates the core business concept of an activity event independently of API DTOs or
 * database persistence entities.
 *
 * @param eventId    Unique identifier for the event (UUID string).
 * @param userId     Unique identifier for the user triggering the event. Also serves as Kafka partition key.
 * @param sessionId  Unique session identifier.
 * @param eventType  Classification of the activity event.
 * @param timestamp  Instant at which the event occurred.
 * @param source     Originating platform or client type.
 * @param metadata   Event-type-specific attributes (e.g., pageUrl, productId, query).
 */
public record ActivityEvent(
        String eventId,
        String userId,
        String sessionId,
        EventType eventType,
        Instant timestamp,
        EventSource source,
        Map<String, Object> metadata
) {
    public ActivityEvent {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId cannot be null or blank");
        }
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId cannot be null or blank");
        }
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("sessionId cannot be null or blank");
        }
        if (eventType == null) {
            throw new IllegalArgumentException("eventType cannot be null");
        }
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp cannot be null");
        }
        if (source == null) {
            throw new IllegalArgumentException("source cannot be null");
        }
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
