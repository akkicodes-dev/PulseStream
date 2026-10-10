package com.pulsestream.application.service;

import com.pulsestream.api.dto.response.AnalyticsOverviewResponse;
import com.pulsestream.api.dto.response.TimeSeriesResponse;
import com.pulsestream.api.dto.response.UserAnalyticsResponse;
import com.pulsestream.infrastructure.persistence.repository.ActivityEventRepository;
import com.pulsestream.infrastructure.persistence.repository.UserSessionRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AnalyticsServiceTest {

    @Mock
    private ActivityEventRepository activityEventRepository;

    @Mock
    private UserSessionRepository userSessionRepository;

    @InjectMocks
    private AnalyticsService analyticsService;

    @Test
    @DisplayName("Should calculate overview metrics successfully within range")
    void shouldCalculateOverviewSuccessfully() {
        OffsetDateTime from = OffsetDateTime.parse("2026-10-10T00:00:00Z");
        OffsetDateTime to = OffsetDateTime.parse("2026-10-10T12:00:00Z");

        when(activityEventRepository.countByEventTimestampBetween(from, to)).thenReturn(150L);
        when(activityEventRepository.countDistinctUserIdByEventTimestampBetween(from, to)).thenReturn(42L);
        when(userSessionRepository.countByIsActiveTrueAndLastSeenAtAfter(any(OffsetDateTime.class))).thenReturn(15L);

        List<Object[]> typeRows = List.<Object[]>of(
                new Object[]{"PAGE_VIEW", 100L},
                new Object[]{"LOGIN", 50L}
        );
        when(activityEventRepository.countByEventTypeBetween(from, to)).thenReturn(typeRows);

        List<Object[]> sourceRows = List.<Object[]>of(
                new Object[]{"web", 120L},
                new Object[]{"mobile", 30L}
        );
        when(activityEventRepository.countBySourceBetween(from, to)).thenReturn(sourceRows);

        AnalyticsOverviewResponse response = analyticsService.getOverview(from, to);

        assertThat(response).isNotNull();
        assertThat(response.totalEvents()).isEqualTo(150L);
        assertThat(response.uniqueUsers()).isEqualTo(42L);
        assertThat(response.activeSessions()).isEqualTo(15L);
        assertThat(response.eventsByType()).containsEntry("PAGE_VIEW", 100L).containsEntry("LOGIN", 50L);
        assertThat(response.eventsBySource()).containsEntry("web", 120L).containsEntry("mobile", 30L);
    }

    @Test
    @DisplayName("Should handle empty dataset gracefully in overview metrics")
    void shouldHandleEmptyDatasetGracefully() {
        OffsetDateTime from = OffsetDateTime.parse("2026-10-10T00:00:00Z");
        OffsetDateTime to = OffsetDateTime.parse("2026-10-10T12:00:00Z");

        when(activityEventRepository.countByEventTimestampBetween(from, to)).thenReturn(0L);
        when(activityEventRepository.countDistinctUserIdByEventTimestampBetween(from, to)).thenReturn(0L);
        when(userSessionRepository.countByIsActiveTrueAndLastSeenAtAfter(any(OffsetDateTime.class))).thenReturn(0L);
        when(activityEventRepository.countByEventTypeBetween(from, to)).thenReturn(Collections.emptyList());
        when(activityEventRepository.countBySourceBetween(from, to)).thenReturn(Collections.emptyList());

        AnalyticsOverviewResponse response = analyticsService.getOverview(from, to);

        assertThat(response).isNotNull();
        assertThat(response.totalEvents()).isZero();
        assertThat(response.uniqueUsers()).isZero();
        assertThat(response.activeSessions()).isZero();
        assertThat(response.eventsByType()).isEmpty();
        assertThat(response.eventsBySource()).isEmpty();
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when from is after to")
    void shouldThrowWhenFromIsAfterTo() {
        OffsetDateTime from = OffsetDateTime.parse("2026-10-10T12:00:00Z");
        OffsetDateTime to = OffsetDateTime.parse("2026-10-10T00:00:00Z");

        assertThatThrownBy(() -> analyticsService.getOverview(from, to))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'from' timestamp cannot be after 'to' timestamp");
    }

    @Test
    @DisplayName("Should handle boundary condition when from equals to (single point in time)")
    void shouldHandleBoundaryConditionWhenFromEqualsTo() {
        OffsetDateTime pointInTime = OffsetDateTime.parse("2026-10-10T12:00:00Z");

        when(activityEventRepository.aggregateTimeSeriesMinute(pointInTime, pointInTime))
                .thenReturn(Collections.emptyList());

        TimeSeriesResponse response = analyticsService.getTimeSeries("MINUTE", pointInTime, pointInTime);

        assertThat(response).isNotNull();
        assertThat(response.bucketCount()).isEqualTo(1);
        assertThat(response.buckets()).hasSize(1);
        assertThat(response.buckets().get(0).timestamp()).isEqualTo(pointInTime);
        assertThat(response.buckets().get(0).totalEvents()).isZero();
    }

    @Test
    @DisplayName("Should enforce interval allowlist (reject invalid intervals)")
    void shouldEnforceIntervalAllowlist() {
        OffsetDateTime from = OffsetDateTime.parse("2026-10-10T00:00:00Z");
        OffsetDateTime to = OffsetDateTime.parse("2026-10-10T04:00:00Z");

        assertThatThrownBy(() -> analyticsService.getTimeSeries("SECOND", from, to))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Allowed intervals are: MINUTE, HOUR, DAY");

        assertThatThrownBy(() -> analyticsService.getTimeSeries("YEAR", from, to))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Allowed intervals are: MINUTE, HOUR, DAY");
    }

    @Test
    @DisplayName("Should enforce maximum time ranges per interval")
    void shouldEnforceMaxTimeRangesPerInterval() {
        OffsetDateTime from = OffsetDateTime.parse("2026-10-10T00:00:00Z");

        // MINUTE max range is 6 hours; request 7 hours -> reject
        OffsetDateTime toMinuteExceeded = from.plusHours(7);
        assertThatThrownBy(() -> analyticsService.getTimeSeries("MINUTE", from, toMinuteExceeded))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum allowed range");

        // HOUR max range is 14 days; request 15 days -> reject
        OffsetDateTime toHourExceeded = from.plusDays(15);
        assertThatThrownBy(() -> analyticsService.getTimeSeries("HOUR", from, toHourExceeded))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum allowed range");

        // DAY max range is 90 days; request 95 days -> reject
        OffsetDateTime toDayExceeded = from.plusDays(95);
        assertThatThrownBy(() -> analyticsService.getTimeSeries("DAY", from, toDayExceeded))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum allowed range");
    }

    @Test
    @DisplayName("Should generate continuous UTC gap-filled time-series buckets")
    void shouldGenerateContinuousUtcGapFilledTimeSeries() {
        OffsetDateTime from = OffsetDateTime.parse("2026-10-10T10:00:00Z");
        OffsetDateTime to = OffsetDateTime.parse("2026-10-10T13:00:00Z"); // 4 buckets: 10:00, 11:00, 12:00, 13:00

        // DB only has data for 11:00
        Timestamp dbBucket = Timestamp.from(OffsetDateTime.parse("2026-10-10T11:00:00Z").toInstant());
        List<Object[]> rows = List.<Object[]>of(
                new Object[]{dbBucket, 25L, 7L}
        );
        when(activityEventRepository.aggregateTimeSeriesHour(from, to)).thenReturn(rows);

        TimeSeriesResponse response = analyticsService.getTimeSeries("HOUR", from, to);

        assertThat(response).isNotNull();
        assertThat(response.interval()).isEqualTo("HOUR");
        assertThat(response.bucketCount()).isEqualTo(4);
        assertThat(response.buckets()).hasSize(4);

        // Bucket 0 (10:00) -> Gap-filled 0
        assertThat(response.buckets().get(0).timestamp()).isEqualTo(OffsetDateTime.parse("2026-10-10T10:00:00Z"));
        assertThat(response.buckets().get(0).totalEvents()).isEqualTo(0L);
        assertThat(response.buckets().get(0).uniqueUsers()).isEqualTo(0L);

        // Bucket 1 (11:00) -> From DB
        assertThat(response.buckets().get(1).timestamp()).isEqualTo(OffsetDateTime.parse("2026-10-10T11:00:00Z"));
        assertThat(response.buckets().get(1).totalEvents()).isEqualTo(25L);
        assertThat(response.buckets().get(1).uniqueUsers()).isEqualTo(7L);

        // Bucket 2 (12:00) -> Gap-filled 0
        assertThat(response.buckets().get(2).timestamp()).isEqualTo(OffsetDateTime.parse("2026-10-10T12:00:00Z"));
        assertThat(response.buckets().get(2).totalEvents()).isEqualTo(0L);

        // Bucket 3 (13:00) -> Gap-filled 0
        assertThat(response.buckets().get(3).timestamp()).isEqualTo(OffsetDateTime.parse("2026-10-10T13:00:00Z"));
        assertThat(response.buckets().get(3).totalEvents()).isEqualTo(0L);
    }

    @Test
    @DisplayName("Should retrieve individual user analytics successfully")
    void shouldRetrieveUserAnalyticsSuccessfully() {
        String userId = "usr-456";

        when(userSessionRepository.countByUserId(userId)).thenReturn(10L);
        when(userSessionRepository.countByUserIdAndIsActiveTrue(userId)).thenReturn(1L);

        Timestamp firstSeen = Timestamp.from(OffsetDateTime.parse("2026-10-01T08:00:00Z").toInstant());
        Timestamp lastSeen = Timestamp.from(OffsetDateTime.parse("2026-10-10T11:30:00Z").toInstant());
        List<Object[]> summary = List.<Object[]>of(
                new Object[]{firstSeen, lastSeen, 85L}
        );
        when(activityEventRepository.findUserSummary(userId)).thenReturn(summary);

        List<Object[]> typeBreakdown = List.<Object[]>of(
                new Object[]{"PAGE_VIEW", 60L},
                new Object[]{"PURCHASE", 25L}
        );
        when(activityEventRepository.countByEventTypeForUser(userId)).thenReturn(typeBreakdown);

        UserAnalyticsResponse response = analyticsService.getUserAnalytics(userId);

        assertThat(response).isNotNull();
        assertThat(response.userId()).isEqualTo(userId);
        assertThat(response.totalEvents()).isEqualTo(85L);
        assertThat(response.totalSessions()).isEqualTo(10L);
        assertThat(response.activeSessions()).isEqualTo(1L);
        assertThat(response.firstSeenAt()).isEqualTo(OffsetDateTime.parse("2026-10-01T08:00:00Z"));
        assertThat(response.lastSeenAt()).isEqualTo(OffsetDateTime.parse("2026-10-10T11:30:00Z"));
        assertThat(response.eventsByType()).containsEntry("PAGE_VIEW", 60L).containsEntry("PURCHASE", 25L);
    }

    @Test
    @DisplayName("Should handle user with no prior activity gracefully")
    void shouldHandleUserWithNoPriorActivityGracefully() {
        String userId = "usr-empty";

        when(userSessionRepository.countByUserId(userId)).thenReturn(0L);
        when(userSessionRepository.countByUserIdAndIsActiveTrue(userId)).thenReturn(0L);
        when(activityEventRepository.findUserSummary(userId)).thenReturn(Collections.emptyList());
        when(activityEventRepository.countByEventTypeForUser(userId)).thenReturn(Collections.emptyList());

        UserAnalyticsResponse response = analyticsService.getUserAnalytics(userId);

        assertThat(response).isNotNull();
        assertThat(response.userId()).isEqualTo(userId);
        assertThat(response.totalEvents()).isZero();
        assertThat(response.totalSessions()).isZero();
        assertThat(response.activeSessions()).isZero();
        assertThat(response.firstSeenAt()).isNull();
        assertThat(response.lastSeenAt()).isNull();
        assertThat(response.eventsByType()).isEmpty();
    }
}
