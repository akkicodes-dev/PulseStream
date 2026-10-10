package com.pulsestream.api.dto.response;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * High-level overview metrics across activity events and sessions.
 */
public record AnalyticsOverviewResponse(
        OffsetDateTime from,
        OffsetDateTime to,
        long totalEvents,
        long uniqueUsers,
        long activeSessions,
        Map<String, Long> eventsByType,
        Map<String, Long> eventsBySource
) {
}
