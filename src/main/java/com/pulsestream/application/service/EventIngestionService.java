package com.pulsestream.application.service;

import com.pulsestream.api.dto.EventRequest;
import com.pulsestream.api.dto.EventResponse;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.messaging.producer.EventProducer;
import lombok.extern.slf4j.Slf4j;
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

    public EventIngestionService(EventProducer eventProducer) {
        this.eventProducer = eventProducer;
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

        return EventResponse.accepted(eventId);
    }

    private String resolveEventId(String providedId) {
        if (providedId == null || providedId.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return providedId.trim();
    }
}
