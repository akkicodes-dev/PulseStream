package com.pulsestream.api.dto.response;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Aggregated time-series data with continuous UTC gap-filled buckets.
 */
public record TimeSeriesResponse(
        String interval,
        OffsetDateTime from,
        OffsetDateTime to,
        int bucketCount,
        List<TimeSeriesBucket> buckets
) {
}
