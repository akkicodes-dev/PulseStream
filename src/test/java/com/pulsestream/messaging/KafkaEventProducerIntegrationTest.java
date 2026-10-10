package com.pulsestream.messaging;

import com.pulsestream.config.JacksonConfig;
import com.pulsestream.config.KafkaProducerConfig;
import com.pulsestream.config.PulseStreamKafkaProperties;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.messaging.producer.KafkaEventProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {
        JacksonConfig.class,
        KafkaProducerConfig.class,
        PulseStreamKafkaProperties.class,
        KafkaEventProducer.class
})
@EmbeddedKafka(
        partitions = 1,
        topics = {"user.activity.events"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestPropertySource(properties = {
        "pulsestream.kafka.topics.activity-events=user.activity.events",
        "pulsestream.kafka.publish-timeout-ms=5000",
        "spring.kafka.producer.acks=all"
})
class KafkaEventProducerIntegrationTest {

    @Autowired
    private KafkaEventProducer kafkaEventProducer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, String> testConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker
        );
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);
        testConsumer = consumerFactory.createConsumer();
        testConsumer.subscribe(Collections.singletonList("user.activity.events"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    @DisplayName("Should publish event to real embedded Kafka broker and verify record key and content")
    void shouldPublishToEmbeddedKafkaSuccessfully() {
        String eventId = UUID.randomUUID().toString();
        String userId = "user_integration_test";
        ActivityEvent event = new ActivityEvent(
                eventId,
                userId,
                "sess_integration",
                EventType.PAGE_VIEW,
                Instant.now(),
                EventSource.WEB,
                Map.of("page", "/test")
        );

        SendResult<String, ActivityEvent> result = kafkaEventProducer.sendEvent(event);

        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().topic()).isEqualTo("user.activity.events");

        ConsumerRecord<String, String> singleRecord =
                KafkaTestUtils.getSingleRecord(testConsumer, "user.activity.events", Duration.ofSeconds(5));

        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo(userId);
        assertThat(singleRecord.value()).contains(eventId);
        assertThat(singleRecord.value()).contains("PAGE_VIEW");
        assertThat(singleRecord.value()).contains("WEB");
    }
}
