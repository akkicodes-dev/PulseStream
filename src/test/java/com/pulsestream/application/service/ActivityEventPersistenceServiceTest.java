package com.pulsestream.application.service;

import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.infrastructure.persistence.entity.ActivityEventEntity;
import com.pulsestream.infrastructure.persistence.repository.ActivityEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ActivityEventPersistenceServiceTest {

    @Mock
    private ActivityEventRepository activityEventRepository;

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
    @DisplayName("Should persist new activity event into PostgreSQL repository")
    void shouldPersistNewEventSuccessfully() {
        when(activityEventRepository.existsByEventId("evt-12345")).thenReturn(false);

        ActivityEventEntity savedEntity = new ActivityEventEntity();
        savedEntity.setId(UUID.randomUUID());
        savedEntity.setEventId(sampleEvent.eventId());
        savedEntity.setUserId(sampleEvent.userId());
        savedEntity.setSessionId(sampleEvent.sessionId());
        savedEntity.setEventType(sampleEvent.eventType());
        savedEntity.setEventTimestamp(sampleEvent.timestamp().atOffset(ZoneOffset.UTC));
        savedEntity.setSource(sampleEvent.source());
        savedEntity.setMetadata(sampleEvent.metadata());
        savedEntity.setProcessed(false);

        when(activityEventRepository.save(any(ActivityEventEntity.class))).thenReturn(savedEntity);

        ActivityEventEntity result = persistenceService.persist(sampleEvent);

        assertThat(result).isNotNull();
        assertThat(result.getEventId()).isEqualTo("evt-12345");
        assertThat(result.getUserId()).isEqualTo("usr-987");
        assertThat(result.isProcessed()).isFalse();

        ArgumentCaptor<ActivityEventEntity> captor = ArgumentCaptor.forClass(ActivityEventEntity.class);
        verify(activityEventRepository).save(captor.capture());
        ActivityEventEntity captured = captor.getValue();
        assertThat(captured.getEventId()).isEqualTo("evt-12345");
        assertThat(captured.getUserId()).isEqualTo("usr-987");
        assertThat(captured.getEventType()).isEqualTo(EventType.PAGE_VIEW);
        assertThat(captured.getSource()).isEqualTo(EventSource.WEB);
    }

    @Test
    @DisplayName("Should detect duplicate eventId via existsByEventId and return existing entity without calling save")
    void shouldHandleDuplicateEventIdIdempotently() {
        when(activityEventRepository.existsByEventId("evt-12345")).thenReturn(true);

        ActivityEventEntity existing = new ActivityEventEntity();
        existing.setId(UUID.randomUUID());
        existing.setEventId("evt-12345");
        existing.setUserId("usr-987");

        when(activityEventRepository.findByEventId("evt-12345")).thenReturn(Optional.of(existing));

        ActivityEventEntity result = persistenceService.persist(sampleEvent);

        assertThat(result).isNotNull();
        assertThat(result.getEventId()).isEqualTo("evt-12345");
        verify(activityEventRepository, never()).save(any(ActivityEventEntity.class));
    }

    @Test
    @DisplayName("Should handle concurrent duplicate insert race condition when save throws DataIntegrityViolationException")
    void shouldHandleDataIntegrityViolationRaceConditionIdempotently() {
        when(activityEventRepository.existsByEventId("evt-12345")).thenReturn(false);
        when(activityEventRepository.save(any(ActivityEventEntity.class)))
                .thenThrow(new DataIntegrityViolationException("duplicate key value violates unique constraint"));

        ActivityEventEntity existing = new ActivityEventEntity();
        existing.setId(UUID.randomUUID());
        existing.setEventId("evt-12345");
        when(activityEventRepository.findByEventId("evt-12345")).thenReturn(Optional.of(existing));

        ActivityEventEntity result = persistenceService.persist(sampleEvent);

        assertThat(result).isNotNull();
        assertThat(result.getEventId()).isEqualTo("evt-12345");
        verify(activityEventRepository).save(any(ActivityEventEntity.class));
    }

    @Test
    @DisplayName("Should propagate database exception when transient DB error occurs to allow consumer retry")
    void shouldPropagateDatabaseExceptionOnTransientFailure() {
        when(activityEventRepository.existsByEventId("evt-12345")).thenReturn(false);
        when(activityEventRepository.save(any(ActivityEventEntity.class)))
                .thenThrow(new QueryTimeoutException("Database connection timeout"));

        assertThatThrownBy(() -> persistenceService.persist(sampleEvent))
                .isInstanceOf(QueryTimeoutException.class)
                .hasMessageContaining("Database connection timeout");
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
