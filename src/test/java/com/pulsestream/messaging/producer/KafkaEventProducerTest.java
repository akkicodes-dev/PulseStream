package com.pulsestream.messaging.producer;

import com.pulsestream.config.PulseStreamKafkaProperties;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.exception.EventPublishingException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaEventProducerTest {

    @Mock
    private KafkaTemplate<String, ActivityEvent> kafkaTemplate;

    private PulseStreamKafkaProperties kafkaProperties;
    private KafkaEventProducer producer;

    @BeforeEach
    void setUp() {
        kafkaProperties = new PulseStreamKafkaProperties();
        kafkaProperties.getTopics().setActivityEvents("user.activity.events");
        kafkaProperties.setPublishTimeoutMs(2000L);

        producer = new KafkaEventProducer(kafkaTemplate, kafkaProperties);
    }

    @Test
    @DisplayName("Should publish event using userId as partition key to the configured topic")
    void shouldPublishToCorrectTopicWithUserIdKey() {
        ActivityEvent event = new ActivityEvent(
                "evt-12345",
                "user_john_doe",
                "sess_abc",
                EventType.LOGIN,
                Instant.now(),
                EventSource.WEB,
                Map.of("ip", "127.0.0.1")
        );

        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("user.activity.events", 2),
                0, 42, System.currentTimeMillis(), 0, 0
        );
        ProducerRecord<String, ActivityEvent> producerRecord = new ProducerRecord<>(
                "user.activity.events", "user_john_doe", event
        );
        SendResult<String, ActivityEvent> sendResult = new SendResult<>(producerRecord, recordMetadata);

        CompletableFuture<SendResult<String, ActivityEvent>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(eq("user.activity.events"), eq("user_john_doe"), eq(event)))
                .thenReturn(future);

        SendResult<String, ActivityEvent> result = producer.sendEvent(event);

        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().topic()).isEqualTo("user.activity.events");
        assertThat(result.getRecordMetadata().partition()).isEqualTo(2);
        assertThat(result.getRecordMetadata().offset()).isEqualTo(42);

        verify(kafkaTemplate).send("user.activity.events", "user_john_doe", event);
    }

    @Test
    @DisplayName("Should throw EventPublishingException when Kafka broker fails with ExecutionException")
    void shouldThrowEventPublishingExceptionOnBrokerFailure() {
        ActivityEvent event = new ActivityEvent(
                "evt-failed",
                "user_error",
                "sess_abc",
                EventType.LOGOUT,
                Instant.now(),
                EventSource.API,
                Map.of()
        );

        CompletableFuture<SendResult<String, ActivityEvent>> future = new CompletableFuture<>();
        future.completeExceptionally(new ExecutionException("Broker Not Available", new RuntimeException("Connection refused")));

        when(kafkaTemplate.send(any(), any(), any())).thenReturn(future);

        assertThatThrownBy(() -> producer.sendEvent(event))
                .isInstanceOf(EventPublishingException.class)
                .hasMessageContaining("Kafka broker rejected or failed to publish event");
    }

    @Test
    @DisplayName("Should throw EventPublishingException when publication times out")
    void shouldThrowEventPublishingExceptionOnTimeout() throws Exception {
        ActivityEvent event = new ActivityEvent(
                "evt-timeout",
                "user_slow",
                "sess_abc",
                EventType.SEARCH,
                Instant.now(),
                EventSource.MOBILE,
                Map.of("query", "shoes")
        );

        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, ActivityEvent>> slowFuture = mock(CompletableFuture.class);
        when(slowFuture.get(eq(2000L), eq(TimeUnit.MILLISECONDS))).thenThrow(new TimeoutException("Timed out"));

        when(kafkaTemplate.send(any(), any(), any())).thenReturn(slowFuture);

        assertThatThrownBy(() -> producer.sendEvent(event))
                .isInstanceOf(EventPublishingException.class)
                .hasMessageContaining("Timed out waiting for Kafka broker acknowledgment after 2000ms");
    }
}
