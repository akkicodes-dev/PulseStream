package com.pulsestream.application.service;

import com.pulsestream.api.dto.response.*;
import com.pulsestream.infrastructure.persistence.entity.UserSessionEntity;
import com.pulsestream.infrastructure.persistence.repository.ActivityEventRepository;
import com.pulsestream.infrastructure.persistence.repository.UserSessionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Service for analytical queries and real-time aggregation metrics.
 *
 * <p>Key Guarantees:
 * <ul>
 *   <li><b>Interval Allowlist:</b> Enforces strict allowlist of intervals ({@code MINUTE}, {@code HOUR}, {@code DAY}).</li>
 *   <li><b>Bounded Work:</b> Enforces maximum duration caps per interval and a hard ceiling of 500 buckets.</li>
 *   <li><b>Consistent UTC Semantics:</b> All timestamps normalized to UTC; range boundaries {@code [from, to]} are documented as inclusive.</li>
 *   <li><b>SQL Injection Immune:</b> Uses static, parameterized repository queries with zero dynamic SQL concatenation.</li>
 *   <li><b>Gap-Filling:</b> Generates continuous time-series buckets with zeroes for missing periods.</li>
 * </ul>
 */
@Service
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class AnalyticsService {

    private final ActivityEventRepository activityEventRepository;
    private final UserSessionRepository userSessionRepository;

    @Value("${pulsestream.session.inactivity-timeout-minutes:30}")
    private long inactivityTimeoutMinutes = 30;

    public enum TimeInterval {
        MINUTE(Duration.ofHours(6), ChronoUnit.MINUTES),
        HOUR(Duration.ofDays(14), ChronoUnit.HOURS),
        DAY(Duration.ofDays(90), ChronoUnit.DAYS);

        private final Duration maxRange;
        private final ChronoUnit chronoUnit;

        TimeInterval(Duration maxRange, ChronoUnit chronoUnit) {
            this.maxRange = maxRange;
            this.chronoUnit = chronoUnit;
        }

        public Duration getMaxRange() {
            return maxRange;
        }

        public ChronoUnit getChronoUnit() {
            return chronoUnit;
        }

        public static Optional<TimeInterval> fromString(String val) {
            if (val == null) {
                return Optional.empty();
            }
            try {
                return Optional.of(TimeInterval.valueOf(val.trim().toUpperCase(Locale.ROOT)));
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        }
    }

    private static final int MAX_BUCKETS_ALLOWED = 500;

    /**
     * Retrieves high-level platform activity metrics within the inclusive range {@code [from, to]}.
     */
    public AnalyticsOverviewResponse getOverview(OffsetDateTime from, OffsetDateTime to) {
        OffsetDateTime nowUtc = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime effectiveTo = to != null ? to.withOffsetSameInstant(ZoneOffset.UTC) : nowUtc;
        OffsetDateTime effectiveFrom = from != null ? from.withOffsetSameInstant(ZoneOffset.UTC) : effectiveTo.minusDays(1);

        if (effectiveFrom.isAfter(effectiveTo)) {
            throw new IllegalArgumentException("'from' timestamp cannot be after 'to' timestamp");
        }

        long totalEvents = activityEventRepository.countByEventTimestampBetween(effectiveFrom, effectiveTo);
        long uniqueUsers = activityEventRepository.countDistinctUserIdByEventTimestampBetween(effectiveFrom, effectiveTo);

        OffsetDateTime activeCutoff = nowUtc.minusMinutes(inactivityTimeoutMinutes);
        long activeSessions = userSessionRepository.countByIsActiveTrueAndLastSeenAtAfter(activeCutoff);

        Map<String, Long> eventsByType = new LinkedHashMap<>();
        for (Object[] row : activityEventRepository.countByEventTypeBetween(effectiveFrom, effectiveTo)) {
            eventsByType.put(String.valueOf(row[0]), ((Number) row[1]).longValue());
        }

        Map<String, Long> eventsBySource = new LinkedHashMap<>();
        for (Object[] row : activityEventRepository.countBySourceBetween(effectiveFrom, effectiveTo)) {
            eventsBySource.put(String.valueOf(row[0]), ((Number) row[1]).longValue());
        }

        return new AnalyticsOverviewResponse(
                effectiveFrom,
                effectiveTo,
                totalEvents,
                uniqueUsers,
                activeSessions,
                eventsByType,
                eventsBySource
        );
    }

    /**
     * Generates a continuous UTC time-series of event counts and unique active users.
     */
    public TimeSeriesResponse getTimeSeries(String intervalStr, OffsetDateTime from, OffsetDateTime to) {
        TimeInterval interval = TimeInterval.fromString(intervalStr)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid interval: '" + intervalStr + "'. Allowed intervals are: MINUTE, HOUR, DAY"));

        OffsetDateTime nowUtc = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime effectiveTo = to != null ? to.withOffsetSameInstant(ZoneOffset.UTC) : nowUtc;
        OffsetDateTime effectiveFrom;

        if (from != null) {
            effectiveFrom = from.withOffsetSameInstant(ZoneOffset.UTC);
        } else {
            effectiveFrom = switch (interval) {
                case MINUTE -> effectiveTo.minusHours(1);
                case HOUR -> effectiveTo.minusDays(1);
                case DAY -> effectiveTo.minusDays(30);
            };
        }

        if (effectiveFrom.isAfter(effectiveTo)) {
            throw new IllegalArgumentException("'from' timestamp cannot be after 'to' timestamp");
        }

        Duration duration = Duration.between(effectiveFrom, effectiveTo);
        if (duration.compareTo(interval.getMaxRange()) > 0) {
            throw new IllegalArgumentException(String.format(
                    "Requested time range (%s) exceeds maximum allowed range of %s for interval '%s'",
                    duration, interval.getMaxRange(), interval.name()));
        }

        // Align boundaries to bucket units
        OffsetDateTime bucketStart = effectiveFrom.truncatedTo(interval.getChronoUnit());
        OffsetDateTime bucketEnd = effectiveTo.truncatedTo(interval.getChronoUnit());

        // Validate bucket count bound before querying
        long estimatedBuckets = switch (interval) {
            case MINUTE -> ChronoUnit.MINUTES.between(bucketStart, bucketEnd) + 1;
            case HOUR -> ChronoUnit.HOURS.between(bucketStart, bucketEnd) + 1;
            case DAY -> ChronoUnit.DAYS.between(bucketStart, bucketEnd) + 1;
        };

        if (estimatedBuckets > MAX_BUCKETS_ALLOWED) {
            throw new IllegalArgumentException(String.format(
                    "Generated bucket count (%d) exceeds hard ceiling of %d buckets",
                    estimatedBuckets, MAX_BUCKETS_ALLOWED));
        }

        // Query database using static parameterized queries
        List<Object[]> rows = switch (interval) {
            case MINUTE -> activityEventRepository.aggregateTimeSeriesMinute(effectiveFrom, effectiveTo);
            case HOUR -> activityEventRepository.aggregateTimeSeriesHour(effectiveFrom, effectiveTo);
            case DAY -> activityEventRepository.aggregateTimeSeriesDay(effectiveFrom, effectiveTo);
        };

        Map<OffsetDateTime, TimeSeriesBucket> rowMap = new HashMap<>();
        for (Object[] row : rows) {
            OffsetDateTime bucketTime = parseBucketTimestamp(row[0]);
            long totalEvents = ((Number) row[1]).longValue();
            long uniqueUsers = ((Number) row[2]).longValue();
            rowMap.put(bucketTime, new TimeSeriesBucket(bucketTime, totalEvents, uniqueUsers));
        }

        // Continuous gap-filling in UTC
        List<TimeSeriesBucket> buckets = new ArrayList<>();
        OffsetDateTime current = bucketStart;
        while (!current.isAfter(bucketEnd)) {
            TimeSeriesBucket existing = rowMap.get(current);
            if (existing != null) {
                buckets.add(existing);
            } else {
                buckets.add(new TimeSeriesBucket(current, 0L, 0L));
            }

            current = switch (interval) {
                case MINUTE -> current.plusMinutes(1);
                case HOUR -> current.plusHours(1);
                case DAY -> current.plusDays(1);
            };
        }

        return new TimeSeriesResponse(
                interval.name(),
                effectiveFrom,
                effectiveTo,
                buckets.size(),
                buckets
        );
    }

    /**
     * Retrieves activity and session metrics for a single user.
     */
    public UserAnalyticsResponse getUserAnalytics(String userId) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId cannot be null or blank");
        }

        long totalSessions = userSessionRepository.countByUserId(userId);
        long activeSessions = userSessionRepository.countByUserIdAndIsActiveTrue(userId);

        List<Object[]> summaryRows = activityEventRepository.findUserSummary(userId);
        OffsetDateTime firstSeenAt = null;
        OffsetDateTime lastSeenAt = null;
        long totalEvents = 0;

        if (!summaryRows.isEmpty() && summaryRows.get(0)[2] != null) {
            Object[] summary = summaryRows.get(0);
            firstSeenAt = summary[0] != null ? parseBucketTimestamp(summary[0]) : null;
            lastSeenAt = summary[1] != null ? parseBucketTimestamp(summary[1]) : null;
            totalEvents = ((Number) summary[2]).longValue();
        }

        Map<String, Long> eventsByType = new LinkedHashMap<>();
        for (Object[] row : activityEventRepository.countByEventTypeForUser(userId)) {
            eventsByType.put(String.valueOf(row[0]), ((Number) row[1]).longValue());
        }

        return new UserAnalyticsResponse(
                userId,
                totalEvents,
                totalSessions,
                activeSessions,
                firstSeenAt,
                lastSeenAt,
                eventsByType
        );
    }

    /**
     * Retrieves paginated active user sessions.
     */
    public Page<ActiveSessionResponse> getActiveSessions(int page, int size) {
        int boundedPage = Math.max(0, page);
        int boundedSize = Math.clamp(size, 1, 100);

        OffsetDateTime activeCutoff = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(inactivityTimeoutMinutes);
        Page<UserSessionEntity> entities = userSessionRepository
                .findByIsActiveTrueAndLastSeenAtAfterOrderByLastSeenAtDesc(activeCutoff, PageRequest.of(boundedPage, boundedSize));

        return entities.map(e -> new ActiveSessionResponse(
                e.getSessionId(),
                e.getUserId(),
                e.getStartedAt(),
                e.getLastSeenAt(),
                e.getEventCount(),
                e.isActive()
        ));
    }

    private OffsetDateTime parseBucketTimestamp(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof OffsetDateTime odt) {
            return odt.withOffsetSameInstant(ZoneOffset.UTC);
        }
        if (obj instanceof Timestamp ts) {
            return ts.toInstant().atOffset(ZoneOffset.UTC);
        }
        if (obj instanceof java.time.LocalDateTime ldt) {
            return ldt.atOffset(ZoneOffset.UTC);
        }
        if (obj instanceof java.time.Instant inst) {
            return inst.atOffset(ZoneOffset.UTC);
        }
        String str = obj.toString();
        try {
            return OffsetDateTime.parse(str).withOffsetSameInstant(ZoneOffset.UTC);
        } catch (java.time.format.DateTimeParseException e) {
            return java.time.LocalDateTime.parse(str).atOffset(ZoneOffset.UTC);
        }
    }
}
