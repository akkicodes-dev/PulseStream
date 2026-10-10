package com.pulsestream.application.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.infrastructure.persistence.entity.ActivityEventEntity;
import com.pulsestream.infrastructure.persistence.repository.ActivityEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import com.pulsestream.infrastructure.metrics.PulseStreamMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.Optional;

/**
 * Service responsible for persisting validated activity events to PostgreSQL and triggering
 * real-time session aggregation.
 *
 * <p>Key Guarantees:
 * <ul>
 *   <li><b>Transaction-Safe Idempotency:</b> Employs native {@code ON CONFLICT (event_id) DO NOTHING}
 *       via {@link JdbcTemplate}. Unlike catching {@code DataIntegrityViolationException} in JPA,
 *       this does NOT mark the physical database transaction as rollback-only.</li>
 *   <li><b>Atomic Event &amp; Session Aggregation:</b> Session aggregation runs ONLY when a genuinely new
 *       event row is inserted (affected rows = 1). Both writes share the exact same transaction boundary;
 *       if session aggregation fails, the event insertion is also rolled back.</li>
 *   <li><b>Duplicate Concurrency Guard:</b> If a duplicate event arrives concurrently, the native conflict
 *       resolves cleanly (affected rows = 0) without triggering session increments or corrupting session lifecycle.</li>
 * </ul>
 */
@Service
@Slf4j
public class ActivityEventPersistenceService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final ActivityEventRepository activityEventRepository;
    private final UserSessionAggregationService userSessionAggregationService;
    private final PulseStreamMetrics metrics;

    public ActivityEventPersistenceService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            ActivityEventRepository activityEventRepository,
            UserSessionAggregationService userSessionAggregationService) {
        this(jdbcTemplate, objectMapper, activityEventRepository, userSessionAggregationService, null);
    }

    @Autowired
    public ActivityEventPersistenceService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            ActivityEventRepository activityEventRepository,
            UserSessionAggregationService userSessionAggregationService,
            @Nullable PulseStreamMetrics metrics) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.activityEventRepository = activityEventRepository;
        this.userSessionAggregationService = userSessionAggregationService;
        this.metrics = metrics;
    }

    private static final String INSERT_EVENT_SQL =
            "INSERT INTO activity_events " +
            "(id, event_id, user_id, session_id, event_type, event_timestamp, source, metadata, processed, received_at) " +
            "VALUES (gen_random_uuid(), ?, ?, ?, ?, ?, ?, ?::jsonb, false, now()) " +
            "ON CONFLICT (event_id) DO NOTHING";

    /**
     * Persists a domain {@link ActivityEvent} into PostgreSQL idempotently and updates the associated user session.
     *
     * @param event Validated activity event received from the Kafka pipeline
     * @return The persisted or existing {@link ActivityEventEntity}
     * @throws IllegalArgumentException if event or required fields are null
     */
    @Transactional
    public ActivityEventEntity persist(ActivityEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("ActivityEvent cannot be null");
        }
        if (event.eventId() == null || event.eventId().isBlank()) {
            throw new IllegalArgumentException("ActivityEvent eventId cannot be null or blank");
        }

        String metadataJson;
        try {
            metadataJson = (event.metadata() != null && !event.metadata().isEmpty())
                    ? objectMapper.writeValueAsString(event.metadata())
                    : null;
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize metadata to JSON for eventId={}", event.eventId(), e);
            throw new IllegalArgumentException("Invalid event metadata JSON", e);
        }

        // Execute transaction-safe atomic insert with conflict guard
        int affectedRows;
        try {
            affectedRows = jdbcTemplate.update(
                    INSERT_EVENT_SQL,
                    event.eventId(),
                    event.userId(),
                    event.sessionId(),
                    event.eventType().name(),
                    Timestamp.from(event.timestamp()),
                    event.source().name().toLowerCase(java.util.Locale.ROOT),
                    metadataJson
            );
        } catch (RuntimeException e) {
            if (metrics != null) {
                metrics.recordPersistenceFailure("db_error");
            }
            throw e;
        }

        if (affectedRows == 1) {
            log.debug("Successfully persisted new ActivityEvent: eventId={}", event.eventId());
            // Atomic session aggregation: runs only for genuinely new event inserts
            userSessionAggregationService.recordEventInSession(event);
            if (metrics != null) {
                metrics.recordEventPersisted(event.eventType().name(), event.source().name());
            }
        } else {
            log.info("Duplicate event detected (eventId={}). Conflict handled safely without session re-aggregation.",
                    event.eventId());
            if (metrics != null) {
                metrics.recordEventDuplicate(event.source().name());
            }
        }

        return activityEventRepository.findByEventId(event.eventId()).orElse(null);
    }

    /**
     * Finds an activity event by its unique eventId.
     *
     * @param eventId the client/server UUID
     * @return Optional containing the entity if found
     */
    @Transactional(readOnly = true)
    public Optional<ActivityEventEntity> findByEventId(String eventId) {
        if (eventId == null || eventId.isBlank()) {
            return Optional.empty();
        }
        return activityEventRepository.findByEventId(eventId);
    }
}
