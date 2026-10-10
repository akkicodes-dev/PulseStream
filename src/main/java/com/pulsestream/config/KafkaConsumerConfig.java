package com.pulsestream.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.messaging.consumer.DlqPayloadSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer configuration for the activity event streaming pipeline.
 *
 * <p>Architectural Guarantees:
 * <ul>
 *   <li><b>Safe Deserialization:</b> {@link ErrorHandlingDeserializer} captures poison messages
 *       without dropping the listener consumer thread.</li>
 *   <li><b>Bounded Retries:</b> {@link DefaultErrorHandler} retries transient errors (e.g. temporary
 *       database unavailability) up to 2 times with 1-second backoff.</li>
 *   <li><b>Dead Letter Queue (DLQ):</b> Unrecoverable poison pills or exhausted retries are
 *       published to the topic {@code user.activity.dlq} with diagnostic headers.</li>
 *   <li><b>Offset Commit After Persistence:</b> {@code AckMode.RECORD} guarantees that the Kafka
 *       offset is committed only after the database transaction commits successfully.</li>
 * </ul>
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:ps-activity-consumer}")
    private String groupId;

    @Value("${pulsestream.kafka.topics.dlq:user.activity.dlq}")
    private String dlqTopic;

    @Value("${pulsestream.kafka.consumer.auto-startup:true}")
    private boolean autoStartup;

    @Value("${pulsestream.kafka.consumer.retry-attempts:2}")
    private long retryAttempts;

    @Value("${pulsestream.kafka.consumer.retry-interval-ms:1000}")
    private long retryIntervalMs;

    @Bean
    public ConsumerFactory<String, ActivityEvent> activityEventConsumerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<ActivityEvent> jsonDeserializer = new JsonDeserializer<>(ActivityEvent.class, objectMapper, false);
        jsonDeserializer.addTrustedPackages("com.pulsestream.*");

        ErrorHandlingDeserializer<String> keyDeserializer = new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<ActivityEvent> valueDeserializer = new ErrorHandlingDeserializer<>(jsonDeserializer);

        return new DefaultKafkaConsumerFactory<>(props, keyDeserializer, valueDeserializer);
    }

    @Bean
    public ProducerFactory<Object, Object> dlqProducerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        DefaultKafkaProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setValueSerializer(new DlqPayloadSerializer(objectMapper));
        return factory;
    }

    @Bean
    public KafkaTemplate<Object, Object> dlqKafkaTemplate(ProducerFactory<Object, Object> dlqProducerFactory) {
        return new KafkaTemplate<>(dlqProducerFactory);
    }

    @Bean
    public DeadLetterPublishingRecoverer activityEventDlqRecoverer(
            KafkaTemplate<Object, Object> dlqKafkaTemplate,
            @org.springframework.lang.Nullable com.pulsestream.infrastructure.metrics.PulseStreamMetrics metrics) {
        return new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (consumerRecord, exception) -> {
                    if (metrics != null) {
                        metrics.recordDlqRouted(dlqTopic, exception != null ? exception.getClass().getSimpleName() : "unknown");
                    }
                    return new TopicPartition(dlqTopic, 0);
                });
    }

    @Bean
    public DefaultErrorHandler activityEventErrorHandler(DeadLetterPublishingRecoverer activityEventDlqRecoverer) {
        FixedBackOff backOff = new FixedBackOff(retryIntervalMs, retryAttempts);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(activityEventDlqRecoverer, backOff);

        // Immediate routing to DLQ for non-retryable poison messages
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                IllegalArgumentException.class
        );
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ActivityEvent> activityEventKafkaListenerContainerFactory(
            ConsumerFactory<String, ActivityEvent> activityEventConsumerFactory,
            DefaultErrorHandler activityEventErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, ActivityEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(activityEventConsumerFactory);
        factory.setCommonErrorHandler(activityEventErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setAutoStartup(autoStartup);

        return factory;
    }
}
