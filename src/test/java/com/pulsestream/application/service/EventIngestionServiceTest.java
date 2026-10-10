package com.pulsestream.application.service;

import com.pulsestream.api.dto.EventRequest;
import com.pulsestream.api.dto.EventResponse;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.messaging.producer.EventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EventIngestionServiceTest {

    @Mock
    private EventProducer eventProducer;

    private EventIngestionService eventIngestionService;

    @BeforeEach
    void setUp() {
        eventIngestionService = new EventIngestionService(eventProducer);
    }

    @Test
    @DisplayName("Should generate a valid UUID eventId when client omits eventId")
    void shouldGenerateEventIdWhenOmitted() {
        // Arrange: client omits eventId
        EventRequest request = new EventRequest(
                null,
                "user_123",
                "sess_abc",
                EventType.PAGE_VIEW,
                Instant.parse("2026-10-09T10:15:30Z"),
                EventSource.WEB,
                Map.of("page", "/home")
        );

        // Act
        EventResponse response = eventIngestionService.ingestEvent(request);

        // Assert response
        assertThat(response.eventId()).isNotNull().isNotBlank();
        assertThat(UUID.fromString(response.eventId())).isNotNull(); // validates UUID format
        assertThat(response.status()).isEqualTo("ACCEPTED");

        // Assert domain event dispatched to producer
        ArgumentCaptor<ActivityEvent> captor = ArgumentCaptor.forClass(ActivityEvent.class);
        verify(eventProducer).sendEvent(captor.capture());

        ActivityEvent publishedEvent = captor.getValue();
        assertThat(publishedEvent.eventId()).isEqualTo(response.eventId());
        assertThat(publishedEvent.userId()).isEqualTo("user_123");
        assertThat(publishedEvent.sessionId()).isEqualTo("sess_abc");
        assertThat(publishedEvent.eventType()).isEqualTo(EventType.PAGE_VIEW);
        assertThat(publishedEvent.source()).isEqualTo(EventSource.WEB);
        assertThat(publishedEvent.metadata()).containsEntry("page", "/home");
    }

    @Test
    @DisplayName("Should preserve client-provided eventId when present")
    void shouldPreserveClientProvidedEventId() {
        String existingId = "550e8400-e29b-41d4-a716-446655440000";
        EventRequest request = new EventRequest(
                existingId,
                "user_456",
                "sess_def",
                EventType.PURCHASE,
                Instant.parse("2026-10-09T10:20:00Z"),
                EventSource.MOBILE,
                Map.of("amount", 99.95)
        );

        EventResponse response = eventIngestionService.ingestEvent(request);

        assertThat(response.eventId()).isEqualTo(existingId);

        ArgumentCaptor<ActivityEvent> captor = ArgumentCaptor.forClass(ActivityEvent.class);
        verify(eventProducer).sendEvent(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo(existingId);
        assertThat(captor.getValue().eventType()).isEqualTo(EventType.PURCHASE);
    }
}
