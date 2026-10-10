package com.pulsestream.api;

import com.pulsestream.infrastructure.metrics.PulseStreamMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=localhost:9999",
        "spring.kafka.streams.bootstrap-servers=localhost:9999",
        "spring.datasource.url=jdbc:postgresql://${POSTGRES_HOST:localhost}:${POSTGRES_PORT:5433}/${POSTGRES_DB:pulsestream}",
        "spring.flyway.enabled=false",
        "spring.jpa.hibernate.ddl-auto=none",
        "pulsestream.kafka.consumer.auto-startup=false",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6380",
        "management.health.elasticsearch.enabled=false",
        "management.health.redis.enabled=false",
        "management.endpoints.web.exposure.include=health,info,metrics,prometheus",
        "management.endpoint.health.probes.enabled=true"
})
class ActuatorObservabilityIntegrationTest {

    @Autowired
    private WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Autowired
    private PulseStreamMetrics metrics;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    @DisplayName("Should return 200 for Actuator health endpoint with status UP")
    void shouldReturnHealthStatusUp() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    @DisplayName("Should return 200 for Actuator liveness probe")
    void shouldReturnLivenessProbeUp() throws Exception {
        mockMvc.perform(get("/actuator/health/liveness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    @DisplayName("Should return 200 for Actuator readiness probe")
    void shouldReturnReadinessProbeUp() throws Exception {
        mockMvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    @DisplayName("Should expose custom PulseStream metrics under /actuator/metrics")
    void shouldExposeCustomPulseStreamMetrics() throws Exception {
        // Record test metrics to ensure they are registered in the registry
        metrics.recordEventAccepted("PAGE_VIEW", "WEB");
        metrics.recordEventPersisted("PAGE_VIEW", "WEB");

        mockMvc.perform(get("/actuator/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.names", hasItem("pulsestream.events.accepted")))
                .andExpect(jsonPath("$.names", hasItem("pulsestream.events.persisted")));
    }

    @Test
    @DisplayName("Should query specific metric details with bounded tags")
    void shouldQuerySpecificMetricDetails() throws Exception {
        metrics.recordEventAccepted("PURCHASE", "API");

        mockMvc.perform(get("/actuator/metrics/pulsestream.events.accepted"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("pulsestream.events.accepted"))
                .andExpect(jsonPath("$.availableTags[?(@.tag == 'event_type')].values", hasItem(hasItem("purchase"))));
    }

    @Test
    @DisplayName("Should scrape Prometheus metrics endpoint format")
    void shouldScrapePrometheusEndpoint() throws Exception {
        metrics.recordEventAccepted("CLICK", "MOBILE");

        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("pulsestream_events_accepted_total")));
    }

    @Test
    @DisplayName("Should refuse access to /actuator/env (not exposed)")
    void shouldNotExposeEnvEndpoint() throws Exception {
        mockMvc.perform(get("/actuator/env"))
                .andExpect(status().isNotFound());
    }
}
