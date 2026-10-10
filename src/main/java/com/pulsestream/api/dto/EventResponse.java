package com.pulsestream.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Response payload returned when an activity event is successfully accepted and published to Kafka.
 */
@Schema(description = "Confirmation response returned upon successful event publication to Kafka")
public record EventResponse(

        @Schema(
                description = "Assigned or accepted unique event ID (UUID string).",
                example = "4f8a3d5e-2b1c-4e8f-9a3d-5e2b1c4e8f9a"
        )
        String eventId,

        @Schema(
                description = "Status of the ingestion request.",
                example = "ACCEPTED"
        )
        String status,

        @Schema(
                description = "Descriptive confirmation message.",
                example = "Event accepted and published to stream"
        )
        String message,

        @Schema(
                description = "Timestamp when the event was processed and published.",
                example = "2026-10-09T10:15:30.123Z"
        )
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp
) {
    public static EventResponse accepted(String eventId) {
        return new EventResponse(
                eventId,
                "ACCEPTED",
                "Event accepted and published to stream",
                Instant.now()
        );
    }
}
