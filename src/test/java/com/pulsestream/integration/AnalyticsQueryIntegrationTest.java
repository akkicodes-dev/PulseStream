package com.pulsestream.integration;

import com.pulsestream.api.dto.response.AnalyticsOverviewResponse;
import com.pulsestream.api.dto.response.TimeSeriesResponse;
import com.pulsestream.api.dto.response.UserAnalyticsResponse;
import com.pulsestream.application.service.AnalyticsService;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.infrastructure.persistence.entity.ActivityEventEntity;
import com.pulsestream.infrastructure.persistence.entity.UserSessionEntity;
import com.pulsestream.infrastructure.persistence.repository.ActivityEventRepository;
import com.pulsestream.infrastructure.persistence.repository.UserSessionRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Database-backed integration test verifying analytics query correctness against PostgreSQL.
 *
 * <p>Validates exact metric totals, unique user counts, event type and source distributions,
 * session calculations, time-series gap-filling, and user lifetime metrics against deterministic datasets.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:postgresql://${POSTGRES_HOST:localhost}:${POSTGRES_PORT:5433}/${POSTGRES_DB:pulsestream}",
        "spring.flyway.enabled=true",
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.kafka.bootstrap-servers=localhost:9999",
        "pulsestream.kafka.consumer.auto-startup=false",
        "management.health.elasticsearch.enabled=false",
        "management.health.redis.enabled=false"
})
class AnalyticsQueryIntegrationTest {

    @Autowired
    private AnalyticsService analyticsService;

    @Autowired
    private ActivityEventRepository activityEventRepository;

    @Autowired
    private UserSessionRepository userSessionRepository;

    private static final String ALICE = "m5-alice-" + UUID.randomUUID();
    private static final String BOB = "m5-bob-" + UUID.randomUUID();
    private static final String SESS_1 = "m5-sess-1-" + UUID.randomUUID();
    private static final String SESS_2 = "m5-sess-2-" + UUID.randomUUID();

    // Use isolated date so platform-wide overview tests don't collide with live test records on 2026-10-10
    private final OffsetDateTime baseTime = OffsetDateTime.parse("2026-11-20T10:00:00Z");

    @BeforeEach
    void seedDeterministicData() {
        cleanUpData();
        // Alice events inside window
        insertEvent("evt-a1", ALICE, SESS_1, EventType.PAGE_VIEW, baseTime.plusMinutes(10), EventSource.WEB);
        insertEvent("evt-a2", ALICE, SESS_1, EventType.PAGE_VIEW, baseTime.plusMinutes(20), EventSource.WEB);
        insertEvent("evt-a3", ALICE, SESS_1, EventType.PURCHASE, baseTime.plusMinutes(30), EventSource.WEB);

        // Bob events inside window
        insertEvent("evt-b1", BOB, SESS_2, EventType.SEARCH, baseTime.plusMinutes(15), EventSource.MOBILE);
        insertEvent("evt-b2", BOB, SESS_2, EventType.LOGIN, baseTime.plusMinutes(25), EventSource.MOBILE);

        // Event outside window (2 days prior)
        insertEvent("evt-out", ALICE, SESS_1, EventType.PAGE_VIEW, baseTime.minusDays(2), EventSource.WEB);

        // Sessions
        insertSession(SESS_1, ALICE, baseTime.plusMinutes(10), baseTime.plusMinutes(30), true, 3);
        insertSession(SESS_2, BOB, baseTime.plusMinutes(15), baseTime.plusMinutes(25), false, 2);
    }

    @AfterEach
    void cleanUpData() {
        for (String eventId : new String[]{"evt-a1", "evt-a2", "evt-a3", "evt-b1", "evt-b2", "evt-out"}) {
            activityEventRepository.findByEventId(eventId).ifPresent(activityEventRepository::delete);
        }
        userSessionRepository.findBySessionId(SESS_1).ifPresent(userSessionRepository::delete);
        userSessionRepository.findBySessionId(SESS_2).ifPresent(userSessionRepository::delete);
    }

    private void insertEvent(String eventId, String userId, String sessionId,
                             EventType type, OffsetDateTime timestamp, EventSource source) {
        ActivityEventEntity entity = new ActivityEventEntity();
        entity.setEventId(eventId);
        entity.setUserId(userId);
        entity.setSessionId(sessionId);
        entity.setEventType(type);
        entity.setEventTimestamp(timestamp);
        entity.setSource(source);
        entity.setProcessed(false);
        activityEventRepository.save(entity);
    }

    private void insertSession(String sessionId, String userId, OffsetDateTime start,
                               OffsetDateTime lastSeen, boolean active, int count) {
        UserSessionEntity session = new UserSessionEntity();
        session.setSessionId(sessionId);
        session.setUserId(userId);
        session.setStartedAt(start);
        session.setLastSeenAt(lastSeen);
        session.setActive(active);
        session.setEventCount(count);
        if (!active) {
            session.setEndedAt(lastSeen);
        }
        userSessionRepository.save(session);
    }

    @Test
    @DisplayName("Should accurately calculate overview totals, distinct users, and breakdowns from seeded data")
    void shouldCalculateAccurateOverviewMetrics() {
        OffsetDateTime from = baseTime;
        OffsetDateTime to = baseTime.plusHours(1);

        AnalyticsOverviewResponse overview = analyticsService.getOverview(from, to);

        assertThat(overview).isNotNull();
        // 5 events inside the 1-hour window (evt-out is excluded)
        assertThat(overview.totalEvents()).isEqualTo(5);
        // 2 distinct users: Alice and Bob
        assertThat(overview.uniqueUsers()).isEqualTo(2);

        // Event type breakdown
        assertThat(overview.eventsByType())
                .containsEntry("PAGE_VIEW", 2L)
                .containsEntry("PURCHASE", 1L)
                .containsEntry("SEARCH", 1L)
                .containsEntry("LOGIN", 1L);

        // Source breakdown
        assertThat(overview.eventsBySource())
                .containsEntry("WEB", 3L)
                .containsEntry("MOBILE", 2L);
    }

    @Test
    @DisplayName("Should generate continuous gap-filled time-series matching known seeded hours")
    void shouldGenerateContinuousGapFilledTimeSeries() {
        OffsetDateTime from = baseTime;
        OffsetDateTime to = baseTime.plusHours(3); // 4 buckets: +0h, +1h, +2h, +3h

        TimeSeriesResponse response = analyticsService.getTimeSeries("HOUR", from, to);

        assertThat(response).isNotNull();
        assertThat(response.bucketCount()).isEqualTo(4);
        assertThat(response.buckets()).hasSize(4);

        // Bucket 0 (12:00) contains all 5 events
        assertThat(response.buckets().get(0).timestamp()).isEqualTo(baseTime.withOffsetSameInstant(ZoneOffset.UTC));
        assertThat(response.buckets().get(0).totalEvents()).isEqualTo(5);
        assertThat(response.buckets().get(0).uniqueUsers()).isEqualTo(2);

        // Buckets 1, 2, 3 have no data -> gap-filled 0
        for (int i = 1; i <= 3; i++) {
            assertThat(response.buckets().get(i).totalEvents()).isZero();
            assertThat(response.buckets().get(i).uniqueUsers()).isZero();
        }
    }

    @Test
    @DisplayName("Should compute exact lifetime metrics and event breakdown for individual user")
    void shouldComputeAccurateUserAnalytics() {
        UserAnalyticsResponse aliceAnalytics = analyticsService.getUserAnalytics(ALICE);

        assertThat(aliceAnalytics).isNotNull();
        assertThat(aliceAnalytics.userId()).isEqualTo(ALICE);
        // Alice has 4 total events (including evt-out)
        assertThat(aliceAnalytics.totalEvents()).isEqualTo(4);
        assertThat(aliceAnalytics.totalSessions()).isEqualTo(1);
        assertThat(aliceAnalytics.activeSessions()).isEqualTo(1);
        assertThat(aliceAnalytics.firstSeenAt()).isEqualTo(baseTime.minusDays(2).withOffsetSameInstant(ZoneOffset.UTC));
        assertThat(aliceAnalytics.lastSeenAt()).isEqualTo(baseTime.plusMinutes(30).withOffsetSameInstant(ZoneOffset.UTC));

        assertThat(aliceAnalytics.eventsByType())
                .containsEntry("PAGE_VIEW", 3L)
                .containsEntry("PURCHASE", 1L);
    }
}
