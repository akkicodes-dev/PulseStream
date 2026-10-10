package com.pulsestream.api.dto.response;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Historical activity analytics for an individual user.
 */
public record UserAnalyticsResponse(
        String userId,
        long totalEvents,
        long totalSessions,
        long activeSessions,
        OffsetDateTime firstSeenAt,
        OffsetDateTime lastSeenAt,
        Map<String, Long> eventsByType
) {
}
