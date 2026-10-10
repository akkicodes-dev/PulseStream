package com.pulsestream.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsestream.domain.event.ActivityEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Producer Configuration for publishing activity events.
 *
 * <p>Delivery Guarantees and Architecture Notes:
 * <ul>
 *   <li><b>acks=all:</b> Guarantees that the leader and all in-sync replicas have committed the message
 *       before acknowledging receipt back to the producer.</li>
 *   <li><b>enable.idempotence=true:</b> Prevents duplicate messages produced due to network retries by
 *       assigning each producer a PID and sequence numbers to records.</li>
 *   <li><b>retries=3:</b> Automatically retries transient broker network errors up to 3 times before failing.</li>
 *   <li><b>max.in.flight.requests.per.connection=1:</b> Preserves strict per-partition ordering even on retry.</li>
 *   <li><b>StringSerializer / JsonSerializer:</b> Custom serialization without Spring type headers to keep
 *       message payloads vendor-agnostic and interoperable with non-Spring consumers.</li>
 * </ul>
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.acks:all}")
    private String acks;

    @Value("${spring.kafka.producer.retries:3}")
    private int retries;

    @Bean
    public ProducerFactory<String, ActivityEvent> activityEventProducerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        DefaultKafkaProducerFactory<String, ActivityEvent> factory = new DefaultKafkaProducerFactory<>(props);
        JsonSerializer<ActivityEvent> jsonSerializer = new JsonSerializer<>(objectMapper);
        jsonSerializer.setAddTypeInfo(false);
        factory.setValueSerializer(jsonSerializer);

        return factory;
    }

    @Bean
    public KafkaTemplate<String, ActivityEvent> activityEventKafkaTemplate(
            ProducerFactory<String, ActivityEvent> activityEventProducerFactory) {
        return new KafkaTemplate<>(activityEventProducerFactory);
    }
}
