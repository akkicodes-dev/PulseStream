package com.pulsestream.infrastructure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Centralized, bounded metric instrumentation for the PulseStream platform.
 *
 * <p>Architectural Invariants:
 * <ul>
 *   <li><b>Strictly Bounded Cardinality:</b> Only low-cardinality static tags (event type, source,
 *       pre-defined error reasons, endpoint names) are permitted. User IDs, session IDs, event IDs,
 *       and raw URLs must NEVER be used as metric labels.</li>
 *   <li><b>Production Reliability:</b> Metric registration and recording must never throw exceptions
 *       or disrupt application transaction boundaries.</li>
 * </ul>
 */
@Component
public class PulseStreamMetrics {

    private final MeterRegistry meterRegistry;

    // Metric names
    public static final String METRIC_EVENTS_ACCEPTED = "pulsestream.events.accepted";
    public static final String METRIC_EVENTS_REJECTED = "pulsestream.events.rejected";
    public static final String METRIC_EVENTS_PERSISTED = "pulsestream.events.persisted";
    public static final String METRIC_EVENTS_DUPLICATE = "pulsestream.events.duplicate";
    public static final String METRIC_PERSISTENCE_FAILURES = "pulsestream.events.persistence.failures";
    public static final String METRIC_CONSUMER_FAILURES = "pulsestream.consumer.failures";
    public static final String METRIC_DLQ_ROUTED = "pulsestream.events.dlq";
    public static final String METRIC_SESSIONS_CREATED = "pulsestream.sessions.created";
    public static final String METRIC_SESSIONS_CLOSED = "pulsestream.sessions.closed";
    public static final String METRIC_ANALYTICS_LATENCY = "pulsestream.analytics.requests";

    public PulseStreamMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Records an event successfully accepted by the ingestion API and published to Kafka.
     */
    public void recordEventAccepted(String eventType, String source) {
        Counter.builder(METRIC_EVENTS_ACCEPTED)
                .description("Total number of activity events accepted by the ingestion API")
                .tag("event_type", sanitize(eventType))
                .tag("source", sanitize(source))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Records an event rejected at the ingestion API boundary (validation error, malformed JSON, broker outage).
     */
    public void recordEventRejected(String reason) {
        Counter.builder(METRIC_EVENTS_REJECTED)
                .description("Total number of activity events rejected by the ingestion API")
                .tag("reason", sanitize(reason))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Records a newly persisted activity event in PostgreSQL.
     */
    public void recordEventPersisted(String eventType, String source) {
        Counter.builder(METRIC_EVENTS_PERSISTED)
                .description("Total number of activity events successfully persisted to PostgreSQL")
                .tag("event_type", sanitize(eventType))
                .tag("source", sanitize(source))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Records an idempotent duplicate event delivery where insertion was skipped.
     */
    public void recordEventDuplicate(String source) {
        Counter.builder(METRIC_EVENTS_DUPLICATE)
                .description("Total number of duplicate activity event deliveries skipped idempotently")
                .tag("source", sanitize(source))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Records an event persistence failure (e.g. database serialization error, SQL failure).
     */
    public void recordPersistenceFailure(String reason) {
        Counter.builder(METRIC_PERSISTENCE_FAILURES)
                .description("Total number of activity event persistence failures")
                .tag("reason", sanitize(reason))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Records a Kafka consumer message processing failure.
     */
    public void recordConsumerFailure(String reason) {
        Counter.builder(METRIC_CONSUMER_FAILURES)
                .description("Total number of consumer errors encountered during event processing")
                .tag("reason", sanitize(reason))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Records an event routed to the Dead-Letter Queue (DLQ).
     */
    public void recordDlqRouted(String topic, String reason) {
        Counter.builder(METRIC_DLQ_ROUTED)
                .description("Total number of events routed to the dead-letter queue")
                .tag("dlq_topic", sanitize(topic))
                .tag("reason", sanitize(reason))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Records a newly established user session.
     */
    public void recordSessionCreated() {
        meterRegistry.counter(METRIC_SESSIONS_CREATED, "status", "created").increment();
    }

    /**
     * Records a closed user session (via explicit LOGOUT event or inactivity expiration sweep).
     */
    public void recordSessionClosed(String reason) {
        Counter.builder(METRIC_SESSIONS_CLOSED)
                .description("Total number of user sessions closed")
                .tag("reason", sanitize(reason))
                .register(meterRegistry)
                .increment();
    }

    /**
     * Times an analytics query execution and records its duration and outcome.
     */
    public <T> T recordAnalyticsTimer(String endpoint, Supplier<T> queryAction) {
        long start = System.nanoTime();
        String status = "success";
        try {
            return queryAction.get();
        } catch (RuntimeException e) {
            status = "error";
            throw e;
        } finally {
            long duration = System.nanoTime() - start;
            Timer.builder(METRIC_ANALYTICS_LATENCY)
                    .description("Execution latency and request count for analytics endpoints")
                    .tag("endpoint", sanitize(endpoint))
                    .tag("status", status)
                    .register(meterRegistry)
                    .record(Duration.ofNanos(duration));
        }
    }

    private String sanitize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }
}
