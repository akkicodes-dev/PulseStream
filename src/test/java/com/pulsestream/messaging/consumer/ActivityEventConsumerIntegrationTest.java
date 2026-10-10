package com.pulsestream.messaging.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsestream.application.service.ActivityEventPersistenceService;
import com.pulsestream.config.JacksonConfig;
import com.pulsestream.config.KafkaConsumerConfig;
import com.pulsestream.config.KafkaProducerConfig;
import com.pulsestream.config.PulseStreamKafkaProperties;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {
        JacksonConfig.class,
        KafkaProducerConfig.class,
        KafkaConsumerConfig.class,
        PulseStreamKafkaProperties.class,
        ActivityEventConsumer.class
})
@EmbeddedKafka(
        partitions = 1,
        topics = {"user.activity.events", "user.activity.dlq"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestPropertySource(properties = {
        "spring.kafka.consumer.group-id=test-consumer-group",
        "pulsestream.kafka.topics.activity-events=user.activity.events",
        "pulsestream.kafka.topics.dlq=user.activity.dlq",
        "pulsestream.kafka.consumer.auto-startup=true",
        "pulsestream.kafka.consumer.retry-attempts=2",
        "pulsestream.kafka.consumer.retry-interval-ms=100",
        "spring.kafka.producer.acks=all"
})
class ActivityEventConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<String, ActivityEvent> activityEventKafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private ActivityEventPersistenceService persistenceService;

    private Consumer<String, String> dlqConsumer;
    private KafkaTemplate<String, String> rawStringKafkaTemplate;

    @BeforeEach
    void setUp() {
        // Consumer for DLQ topic
        Map<String, Object> dlqConsumerProps = KafkaTestUtils.consumerProps(
                "dlq-test-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker
        );
        dlqConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        dlqConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        dlqConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(dlqConsumerProps);
        dlqConsumer = consumerFactory.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dlqConsumer, "user.activity.dlq");
        dlqConsumer.poll(Duration.ofMillis(200));
        dlqConsumer.seekToEnd(dlqConsumer.assignment());

        // Raw producer for sending malformed / poison string payloads
        Map<String, Object> rawProducerProps = new HashMap<>();
        rawProducerProps.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString());
        rawProducerProps.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        rawProducerProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);

        rawStringKafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(rawProducerProps));
    }

    @AfterEach
    void tearDown() {
        if (dlqConsumer != null) {
            dlqConsumer.close();
        }
    }

    @Test
    @DisplayName("Should consume valid ActivityEvent from user.activity.events and persist successfully")
    void shouldConsumeAndPersistValidEvent() {
        String eventId = UUID.randomUUID().toString();
        String userId = "usr_integration_consumer";
        ActivityEvent event = new ActivityEvent(
                eventId,
                userId,
                "sess_123",
                EventType.PAGE_VIEW,
                Instant.now(),
                EventSource.WEB,
                Map.of("page", "/dashboard")
        );

        activityEventKafkaTemplate.send("user.activity.events", userId, event);

        verify(persistenceService, timeout(5000).times(1)).persist(argThat(e ->
                e.eventId().equals(eventId) && e.userId().equals(userId)));
    }

    @Test
    @DisplayName("Should route poison pill / malformed JSON directly to DLQ without crashing consumer")
    void shouldRouteMalformedMessageToDlq() {
        String malformedJson = "{ invalid_json_syntax: true ";
        String key = "malformed_key_1";

        rawStringKafkaTemplate.send("user.activity.events", key, malformedJson);

        ConsumerRecord<String, String> dlqRecord = KafkaTestUtils.getSingleRecord(
                dlqConsumer, "user.activity.dlq", Duration.ofSeconds(10));

        assertThat(dlqRecord).isNotNull();
        assertThat(dlqRecord.topic()).isEqualTo("user.activity.dlq");
        assertThat(dlqRecord.value()).contains("invalid_json_syntax");
    }

    @Test
    @DisplayName("Should retry on transient database failure and route to DLQ when retries are exhausted")
    void shouldRetryAndRouteToDlqOnDatabaseFailure() {
        String eventId = UUID.randomUUID().toString();
        String userId = "usr_db_fail";
        ActivityEvent event = new ActivityEvent(
                eventId,
                userId,
                "sess_fail",
                EventType.SEARCH,
                Instant.now(),
                EventSource.API,
                Map.of("query", "error-test")
        );

        doThrow(new TransientDataAccessResourceException("Database connection timeout"))
                .when(persistenceService).persist(any());

        activityEventKafkaTemplate.send("user.activity.events", userId, event);

        // Verify retries occurred (1 initial attempt + 2 configured retries = 3 total invocations)
        verify(persistenceService, timeout(10000).atLeast(3)).persist(any());

        // Verify message was routed to DLQ after retries exhausted
        ConsumerRecord<String, String> dlqRecord = KafkaTestUtils.getSingleRecord(
                dlqConsumer, "user.activity.dlq", Duration.ofSeconds(10));

        assertThat(dlqRecord).isNotNull();
        assertThat(dlqRecord.topic()).isEqualTo("user.activity.dlq");
        assertThat(dlqRecord.value()).contains(eventId);
    }
}
