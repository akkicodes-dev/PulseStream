package com.pulsestream.infrastructure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PulseStreamMetricsTest {

    private MeterRegistry meterRegistry;
    private PulseStreamMetrics metrics;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        metrics = new PulseStreamMetrics(meterRegistry);
    }

    @Test
    @DisplayName("Should increment events accepted counter with sanitized tags")
    void shouldIncrementEventsAccepted() {
        metrics.recordEventAccepted("PAGE_VIEW", "WEB");

        Counter counter = meterRegistry.find(PulseStreamMetrics.METRIC_EVENTS_ACCEPTED)
                .tag("event_type", "page_view")
                .tag("source", "web")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should increment events rejected counter with reason tag")
    void shouldIncrementEventsRejected() {
        metrics.recordEventRejected("validation_error");

        Counter counter = meterRegistry.find(PulseStreamMetrics.METRIC_EVENTS_REJECTED)
                .tag("reason", "validation_error")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should sanitize null or blank tags to 'unknown'")
    void shouldSanitizeNullOrBlankTags() {
        metrics.recordEventAccepted(null, "  ");

        Counter counter = meterRegistry.find(PulseStreamMetrics.METRIC_EVENTS_ACCEPTED)
                .tag("event_type", "unknown")
                .tag("source", "unknown")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should record persisted, duplicate, and persistence failure counters")
    void shouldRecordPersistenceCounters() {
        metrics.recordEventPersisted("PURCHASE", "API");
        metrics.recordEventDuplicate("MOBILE");
        metrics.recordPersistenceFailure("db_timeout");

        Counter persisted = meterRegistry.find(PulseStreamMetrics.METRIC_EVENTS_PERSISTED)
                .tag("event_type", "purchase")
                .tag("source", "api")
                .counter();
        assertThat(persisted).isNotNull();
        assertThat(persisted.count()).isEqualTo(1.0);

        Counter duplicate = meterRegistry.find(PulseStreamMetrics.METRIC_EVENTS_DUPLICATE)
                .tag("source", "mobile")
                .counter();
        assertThat(duplicate).isNotNull();
        assertThat(duplicate.count()).isEqualTo(1.0);

        Counter failure = meterRegistry.find(PulseStreamMetrics.METRIC_PERSISTENCE_FAILURES)
                .tag("reason", "db_timeout")
                .counter();
        assertThat(failure).isNotNull();
        assertThat(failure.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should record consumer failures and DLQ routed counters")
    void shouldRecordConsumerAndDlqCounters() {
        metrics.recordConsumerFailure("poison_pill");
        metrics.recordDlqRouted("user.activity.dlq", "deserialization_exception");

        Counter consumerFailure = meterRegistry.find(PulseStreamMetrics.METRIC_CONSUMER_FAILURES)
                .tag("reason", "poison_pill")
                .counter();
        assertThat(consumerFailure).isNotNull();
        assertThat(consumerFailure.count()).isEqualTo(1.0);

        Counter dlq = meterRegistry.find(PulseStreamMetrics.METRIC_DLQ_ROUTED)
                .tag("dlq_topic", "user.activity.dlq")
                .tag("reason", "deserialization_exception")
                .counter();
        assertThat(dlq).isNotNull();
        assertThat(dlq.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should record session lifecycle counters")
    void shouldRecordSessionCounters() {
        metrics.recordSessionCreated();
        metrics.recordSessionClosed("logout");
        metrics.recordSessionClosed("inactivity_sweep");

        Counter created = meterRegistry.find(PulseStreamMetrics.METRIC_SESSIONS_CREATED)
                .tag("status", "created")
                .counter();
        assertThat(created).isNotNull();
        assertThat(created.count()).isEqualTo(1.0);

        Counter closedLogout = meterRegistry.find(PulseStreamMetrics.METRIC_SESSIONS_CLOSED)
                .tag("reason", "logout")
                .counter();
        assertThat(closedLogout).isNotNull();
        assertThat(closedLogout.count()).isEqualTo(1.0);

        Counter closedSweep = meterRegistry.find(PulseStreamMetrics.METRIC_SESSIONS_CLOSED)
                .tag("reason", "inactivity_sweep")
                .counter();
        assertThat(closedSweep).isNotNull();
        assertThat(closedSweep.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should record analytics timer for successful query action")
    void shouldRecordAnalyticsTimerOnSuccess() {
        String result = metrics.recordAnalyticsTimer("overview", () -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {}
            return "ok";
        });

        assertThat(result).isEqualTo("ok");

        Timer timer = meterRegistry.find(PulseStreamMetrics.METRIC_ANALYTICS_LATENCY)
                .tag("endpoint", "overview")
                .tag("status", "success")
                .timer();

        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(5.0);
    }

    @Test
    @DisplayName("Should record analytics timer with status=error when query fails")
    void shouldRecordAnalyticsTimerOnError() {
        assertThatThrownBy(() -> metrics.recordAnalyticsTimer("timeseries", () -> {
            throw new IllegalArgumentException("Invalid interval");
        })).isInstanceOf(IllegalArgumentException.class);

        Timer timer = meterRegistry.find(PulseStreamMetrics.METRIC_ANALYTICS_LATENCY)
                .tag("endpoint", "timeseries")
                .tag("status", "error")
                .timer();

        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1);
    }
}
