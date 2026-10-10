package com.pulsestream.api.dto.response;

import java.time.OffsetDateTime;

/**
 * Representation of an active user session.
 */
public record ActiveSessionResponse(
        String sessionId,
        String userId,
        OffsetDateTime startedAt,
        OffsetDateTime lastSeenAt,
        int eventCount,
        boolean active
) {
}
