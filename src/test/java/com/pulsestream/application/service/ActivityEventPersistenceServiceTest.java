package com.pulsestream.application.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.infrastructure.persistence.entity.ActivityEventEntity;
import com.pulsestream.infrastructure.persistence.repository.ActivityEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ActivityEventPersistenceServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private ActivityEventRepository activityEventRepository;

    @Mock
    private UserSessionAggregationService userSessionAggregationService;

    @InjectMocks
    private ActivityEventPersistenceService persistenceService;

    private ActivityEvent sampleEvent;

    @BeforeEach
    void setUp() {
        sampleEvent = new ActivityEvent(
                "evt-12345",
                "usr-987",
                "sess-abc",
                EventType.PAGE_VIEW,
                Instant.parse("2026-10-10T12:00:00Z"),
                EventSource.WEB,
                Map.of("pageUrl", "/home")
        );
    }

    @Test
    @DisplayName("Should insert new event and trigger session aggregation when insert succeeds (affectedRows = 1)")
    void shouldPersistNewEventAndTriggerSessionAggregation() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(1);

        ActivityEventEntity savedEntity = new ActivityEventEntity();
        savedEntity.setId(UUID.randomUUID());
        savedEntity.setEventId(sampleEvent.eventId());
        savedEntity.setUserId(sampleEvent.userId());
        when(activityEventRepository.findByEventId("evt-12345")).thenReturn(Optional.of(savedEntity));

        ActivityEventEntity result = persistenceService.persist(sampleEvent);

        assertThat(result).isNotNull();
        assertThat(result.getEventId()).isEqualTo("evt-12345");
        verify(userSessionAggregationService, times(1)).recordEventInSession(sampleEvent);
        verify(activityEventRepository, times(1)).findByEventId("evt-12345");
    }

    @Test
    @DisplayName("Should handle duplicate event safely via ON CONFLICT without running session aggregation (affectedRows = 0)")
    void shouldHandleDuplicateEventWithoutTriggeringSessionAggregation() {
        // Affected rows = 0 indicates PostgreSQL ON CONFLICT DO NOTHING resolved a duplicate event_id
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(0);

        ActivityEventEntity existingEntity = new ActivityEventEntity();
        existingEntity.setId(UUID.randomUUID());
        existingEntity.setEventId(sampleEvent.eventId());
        when(activityEventRepository.findByEventId("evt-12345")).thenReturn(Optional.of(existingEntity));

        ActivityEventEntity result = persistenceService.persist(sampleEvent);

        assertThat(result).isNotNull();
        assertThat(result.getEventId()).isEqualTo("evt-12345");
        // Critical safeguard: session aggregation must NEVER run for duplicate events
        verify(userSessionAggregationService, never()).recordEventInSession(any());
        verify(activityEventRepository, times(1)).findByEventId("evt-12345");
    }

    @Test
    @DisplayName("Should propagate exception if session aggregation fails, allowing atomic rollback")
    void shouldPropagateExceptionIfSessionAggregationFails() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(1);
        doThrow(new IllegalStateException("Session update failed"))
                .when(userSessionAggregationService).recordEventInSession(sampleEvent);

        assertThatThrownBy(() -> persistenceService.persist(sampleEvent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Session update failed");

        verify(activityEventRepository, never()).findByEventId(anyString());
    }

    @Test
    @DisplayName("Should propagate database exception when transient DB error occurs")
    void shouldPropagateDatabaseExceptionOnTransientFailure() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any(), any()))
                .thenThrow(new QueryTimeoutException("Database connection timeout"));

        assertThatThrownBy(() -> persistenceService.persist(sampleEvent))
                .isInstanceOf(QueryTimeoutException.class)
                .hasMessageContaining("Database connection timeout");

        verify(userSessionAggregationService, never()).recordEventInSession(any());
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when event is null")
    void shouldThrowIllegalArgumentExceptionWhenEventIsNull() {
        assertThatThrownBy(() -> persistenceService.persist(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ActivityEvent cannot be null");
    }

    @Test
    @DisplayName("Should find event by eventId")
    void shouldFindByEventId() {
        ActivityEventEntity entity = new ActivityEventEntity();
        entity.setEventId("evt-12345");
        when(activityEventRepository.findByEventId("evt-12345")).thenReturn(Optional.of(entity));

        Optional<ActivityEventEntity> found = persistenceService.findByEventId("evt-12345");

        assertThat(found).isPresent();
        assertThat(found.get().getEventId()).isEqualTo("evt-12345");
    }
}
