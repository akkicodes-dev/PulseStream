package com.pulsestream.api.dto.response;

import java.time.OffsetDateTime;

/**
 * A single time-series bucket containing aggregated event counts and unique active users.
 */
public record TimeSeriesBucket(
        OffsetDateTime timestamp,
        long totalEvents,
        long uniqueUsers
) {
}
