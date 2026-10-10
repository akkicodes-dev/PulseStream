package com.pulsestream.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for PulseStream Kafka integration.
 */
@Configuration
@ConfigurationProperties(prefix = "pulsestream.kafka")
@Getter
@Setter
public class PulseStreamKafkaProperties {

    private Topics topics = new Topics();

    /**
     * Maximum time in milliseconds to wait for Kafka broker acknowledgement
     * before timing out and failing the ingestion request.
     */
    private long publishTimeoutMs = 5000L;

    @Getter
    @Setter
    public static class Topics {
        private String activityEvents = "user.activity.events";
        private String processedEvents = "user.activity.processed";
        private String dlq = "user.activity.dlq";
    }
}
