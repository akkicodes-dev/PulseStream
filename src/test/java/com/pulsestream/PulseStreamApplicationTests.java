package com.pulsestream;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Spring Boot application context smoke test.
 *
 * <p>This test verifies that the entire Spring application context loads
 * without errors. It is intentionally minimal for Milestone 1.
 *
 * <p>Note: This test requires all external services (PostgreSQL, Kafka, Redis,
 * Elasticsearch) to be available. In CI, use Testcontainers. For local builds
 * without Docker, override datasource/kafka/redis/es config via test properties.
 *
 * <p>Full integration tests with Testcontainers will be added in Milestone 3+.
 */
@SpringBootTest
@TestPropertySource(properties = {
        // Prevent Kafka from failing the context load during unit test phase
        // (no broker available in unit test environment)
        "spring.kafka.bootstrap-servers=localhost:9999",
        "spring.kafka.streams.bootstrap-servers=localhost:9999",
        // Prevent Flyway from running during context-only tests
        // (full DB tests use Testcontainers)
        "spring.flyway.enabled=false",
        "spring.jpa.hibernate.ddl-auto=none",
        // Prevent Redis from failing
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6380",
        // Prevent Elasticsearch health check from failing
        "management.health.elasticsearch.enabled=false",
        "management.health.redis.enabled=false"
})
class PulseStreamApplicationTests {

    @Test
    void contextLoads() {
        // If we reach this point, the Spring application context loaded successfully.
        // This catches missing beans, misconfigured properties, and circular dependencies.
    }
}
