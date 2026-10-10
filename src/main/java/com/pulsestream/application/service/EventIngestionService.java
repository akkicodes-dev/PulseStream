package com.pulsestream.application.service;

import com.pulsestream.api.dto.EventRequest;
import com.pulsestream.api.dto.EventResponse;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.messaging.producer.EventProducer;
import com.pulsestream.infrastructure.metrics.PulseStreamMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Application service orchestrating the event ingestion pipeline.
 *
 * <p>Handles ID assignment, maps incoming DTOs to pure domain events, and coordinates
 * with the messaging producer to stream events downstream.
 */
@Service
@Slf4j
public class EventIngestionService {

    private final EventProducer eventProducer;
    private final PulseStreamMetrics metrics;

    public EventIngestionService(EventProducer eventProducer) {
        this(eventProducer, null);
    }

    @Autowired
    public EventIngestionService(EventProducer eventProducer, @Nullable PulseStreamMetrics metrics) {
        this.eventProducer = eventProducer;
        this.metrics = metrics;
    }

    /**
     * Ingests a validated activity event request.
     *
     * @param request Validated client request DTO.
     * @return {@link EventResponse} containing the assigned event ID and confirmation.
     */
    public EventResponse ingestEvent(EventRequest request) {
        String eventId = resolveEventId(request.eventId());

        ActivityEvent domainEvent = new ActivityEvent(
                eventId,
                request.userId().trim(),
                request.sessionId().trim(),
                request.eventType(),
                request.timestamp(),
                request.source(),
                request.metadata()
        );

        eventProducer.sendEvent(domainEvent);

        if (metrics != null) {
            metrics.recordEventAccepted(domainEvent.eventType().name(), domainEvent.source().name());
        }

        return EventResponse.accepted(eventId);
    }

    private String resolveEventId(String providedId) {
        if (providedId == null || providedId.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return providedId.trim();
    }
}
