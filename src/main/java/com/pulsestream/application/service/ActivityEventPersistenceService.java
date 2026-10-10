package com.pulsestream.application.service;

import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.infrastructure.persistence.entity.ActivityEventEntity;
import com.pulsestream.infrastructure.persistence.repository.ActivityEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.ZoneOffset;
import java.util.Optional;

/**
 * Service responsible for persisting validated activity events to PostgreSQL.
 *
 * <p>Key Guarantees:
 * <ul>
 *   <li><b>Idempotency:</b> Checks {@link ActivityEventRepository#existsByEventId(String)} before insertion.
 *       Catches {@link DataIntegrityViolationException} in case of concurrent delivery races.</li>
 *   <li><b>Transactional Boundary:</b> Runs within {@code @Transactional} to guarantee atomicity.
 *       Unchecked database errors propagate to trigger Kafka consumer retry mechanisms.</li>
 *   <li><b>Schema Integrity:</b> Reuses existing {@link ActivityEventEntity} and database constraints.</li>
 * </ul>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ActivityEventPersistenceService {

    private final ActivityEventRepository activityEventRepository;

    /**
     * Persists a domain {@link ActivityEvent} into PostgreSQL if not already present.
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

        // 1. Application-level idempotency check
        if (activityEventRepository.existsByEventId(event.eventId())) {
            log.info("Duplicate event detected (eventId={}). Skipping insertion to preserve idempotency.", event.eventId());
            return activityEventRepository.findByEventId(event.eventId()).orElse(null);
        }

        // 2. Map domain event to JPA entity
        ActivityEventEntity entity = new ActivityEventEntity();
        entity.setEventId(event.eventId());
        entity.setUserId(event.userId());
        entity.setSessionId(event.sessionId());
        entity.setEventType(event.eventType());
        entity.setEventTimestamp(event.timestamp().atOffset(ZoneOffset.UTC));
        entity.setSource(event.source());
        entity.setMetadata(event.metadata());
        entity.setProcessed(false);

        // 3. Database persistence with duplicate race guard
        try {
            ActivityEventEntity saved = activityEventRepository.save(entity);
            log.debug("Successfully persisted ActivityEvent to PostgreSQL: eventId={}, id={}", saved.getEventId(), saved.getId());
            return saved;
        } catch (DataIntegrityViolationException ex) {
            log.warn("Database unique constraint violation for eventId={}. Concurrent duplicate delivery handled idempotently.",
                    event.eventId());
            return activityEventRepository.findByEventId(event.eventId()).orElse(null);
        }
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
