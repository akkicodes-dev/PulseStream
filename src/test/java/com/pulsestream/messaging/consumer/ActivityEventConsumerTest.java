package com.pulsestream.messaging.consumer;

import com.pulsestream.application.service.ActivityEventPersistenceService;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.QueryTimeoutException;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ActivityEventConsumerTest {

    @Mock
    private ActivityEventPersistenceService persistenceService;

    @InjectMocks
    private ActivityEventConsumer consumer;

    private ActivityEvent event;

    @BeforeEach
    void setUp() {
        event = new ActivityEvent(
                "evt-999",
                "usr-111",
                "sess-222",
                EventType.PAGE_VIEW,
                Instant.now(),
                EventSource.WEB,
                Map.of("page", "/dashboard")
        );
    }

    @Test
    @DisplayName("Should receive ConsumerRecord and delegate to ActivityEventPersistenceService")
    void shouldProcessAndPersistRecordSuccessfully() {
        ConsumerRecord<String, ActivityEvent> record =
                new ConsumerRecord<>("user.activity.events", 0, 10L, "usr-111", event);

        consumer.onMessage(record);

        verify(persistenceService).persist(event);
    }

    @Test
    @DisplayName("Should safely handle null record payload without delegating to persistence service")
    void shouldIgnoreNullRecordValue() {
        ConsumerRecord<String, ActivityEvent> record =
                new ConsumerRecord<>("user.activity.events", 0, 10L, "usr-111", null);

        consumer.onMessage(record);

        verify(persistenceService, never()).persist(any());
    }

    @Test
    @DisplayName("Should propagate persistence exception to let Spring Kafka DefaultErrorHandler handle retry and DLQ")
    void shouldPropagateExceptionWhenPersistenceFails() {
        ConsumerRecord<String, ActivityEvent> record =
                new ConsumerRecord<>("user.activity.events", 0, 10L, "usr-111", event);

        doThrow(new QueryTimeoutException("Database unreachable"))
                .when(persistenceService).persist(event);

        assertThatThrownBy(() -> consumer.onMessage(record))
                .isInstanceOf(QueryTimeoutException.class)
                .hasMessageContaining("Database unreachable");

        verify(persistenceService).persist(event);
    }
}
