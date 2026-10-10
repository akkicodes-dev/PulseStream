package com.pulsestream.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;

/**
 * Data Transfer Object for incoming user activity event ingestion requests.
 */
@Schema(description = "User activity event payload for ingestion into the streaming pipeline")
public record EventRequest(

        @Schema(
                description = "Optional client-specified or idempotency UUID. If omitted, a UUID is automatically generated.",
                example = "4f8a3d5e-2b1c-4e8f-9a3d-5e2b1c4e8f9a",
                nullable = true
        )
        @Pattern(
                regexp = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
                message = "eventId must be a valid UUID string in 8-4-4-4-12 format"
        )
        String eventId,

        @Schema(
                description = "Unique user identifier. Used as the Kafka message partition key.",
                example = "usr_102938",
                requiredMode = Schema.RequiredMode.REQUIRED,
                maxLength = 255
        )
        @NotBlank(message = "userId is required and cannot be blank")
        @Size(max = 255, message = "userId cannot exceed 255 characters")
        String userId,

        @Schema(
                description = "Session identifier grouping user activities within a browsing session.",
                example = "sess_9876543210",
                requiredMode = Schema.RequiredMode.REQUIRED,
                maxLength = 255
        )
        @NotBlank(message = "sessionId is required and cannot be blank")
        @Size(max = 255, message = "sessionId cannot exceed 255 characters")
        String sessionId,

        @Schema(
                description = "Type of activity event.",
                example = "PAGE_VIEW",
                requiredMode = Schema.RequiredMode.REQUIRED
        )
        @NotNull(message = "eventType is required")
        EventType eventType,

        @Schema(
                description = "Timestamp when the event occurred in ISO-8601 UTC format.",
                example = "2026-10-09T10:15:30Z",
                requiredMode = Schema.RequiredMode.REQUIRED
        )
        @NotNull(message = "timestamp is required")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp,

        @Schema(
                description = "Originating platform for the event.",
                example = "WEB",
                requiredMode = Schema.RequiredMode.REQUIRED
        )
        @NotNull(message = "source is required")
        EventSource source,

        @Schema(
                description = "Optional JSON-compatible map containing event-specific metadata attributes.",
                example = "{\"pageUrl\": \"/products/laptop-pro\", \"referrer\": \"https://search.engine.com\"}",
                nullable = true
        )
        Map<String, Object> metadata
) {
}
