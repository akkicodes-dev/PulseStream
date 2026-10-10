package com.pulsestream.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * Standardized error response body returned across all PulseStream REST APIs.
 */
@Schema(description = "Standardized error response")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(

        @Schema(description = "Timestamp when the error occurred in ISO-8601 UTC format", example = "2026-10-09T10:15:30.123Z")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp,

        @Schema(description = "HTTP status code", example = "400")
        int status,

        @Schema(description = "HTTP reason phrase", example = "Bad Request")
        String error,

        @Schema(description = "Human-readable description of the error", example = "Validation failed for request payload")
        String message,

        @Schema(description = "Request URI path where the error occurred", example = "/api/v1/events")
        String path,

        @Schema(description = "Correlation ID for tracing the request across logs and systems", example = "a1b2c3d4-e5f6-7890-abcd-ef1234567890")
        String requestId,

        @Schema(description = "Detailed list of field-level validation errors or failure specifics", example = "[\"userId: userId is required\"]")
        List<String> details
) {
    public ErrorResponse(int status, String error, String message, String path, String requestId) {
        this(Instant.now(), status, error, message, path, requestId, null);
    }

    public ErrorResponse(int status, String error, String message, String path, String requestId, List<String> details) {
        this(Instant.now(), status, error, message, path, requestId, details);
    }
}
