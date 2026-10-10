package com.pulsestream.api.controller;

import com.pulsestream.api.dto.ErrorResponse;
import com.pulsestream.api.dto.response.ActiveSessionResponse;
import com.pulsestream.api.dto.response.AnalyticsOverviewResponse;
import com.pulsestream.api.dto.response.PageResponse;
import com.pulsestream.api.dto.response.TimeSeriesResponse;
import com.pulsestream.api.dto.response.UserAnalyticsResponse;
import com.pulsestream.application.service.AnalyticsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;

/**
 * REST API controller providing real-time platform analytics, aggregated time-series,
 * per-user metrics, and active session monitoring.
 */
@RestController
@RequestMapping(path = "/api/v1/analytics", produces = MediaType.APPLICATION_JSON_VALUE)
@Tag(name = "Analytics & Aggregation", description = "Endpoints for real-time activity metrics and session insights")
@RequiredArgsConstructor
@Slf4j
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    @GetMapping("/overview")
    @Operation(
            summary = "Get platform activity overview",
            description = """
                    Calculates platform-wide aggregate counts within the inclusive window [from, to].
                    
                    **Metrics Returned:**
                    - Total events ingested
                    - Distinct active users
                    - Active sessions (within inactivity window)
                    - Breakdown by event type
                    - Breakdown by client source (web, mobile, api)
                    """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Overview metrics successfully computed"),
            @ApiResponse(responseCode = "400", description = "Invalid date range parameters",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<AnalyticsOverviewResponse> getOverview(
            @Parameter(description = "Start timestamp (ISO-8601, inclusive). Defaults to 24h prior to 'to'.")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,

            @Parameter(description = "End timestamp (ISO-8601, inclusive). Defaults to current time.")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to
    ) {
        log.debug("GET /api/v1/analytics/overview requested [from={}, to={}]", from, to);
        AnalyticsOverviewResponse overview = analyticsService.getOverview(from, to);
        return ResponseEntity.ok(overview);
    }

    @GetMapping("/timeseries")
    @Operation(
            summary = "Get activity time-series aggregations",
            description = """
                    Returns continuous UTC time-series buckets with zero-filled gaps for missing intervals.
                    
                    **Validation & Safeguards:**
                    - Allowed intervals: `MINUTE` (max range: 6h), `HOUR` (max range: 14d), `DAY` (max range: 90d)
                    - Hard ceiling of 500 buckets to prevent memory exhaustion
                    - Uses static parameterized SQL to guarantee zero dynamic concatenation
                    """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Time-series buckets computed with gap-filling"),
            @ApiResponse(responseCode = "400", description = "Interval not allowed or range exceeds boundary",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<TimeSeriesResponse> getTimeSeries(
            @Parameter(description = "Bucketing interval: MINUTE, HOUR, or DAY. Default is HOUR.")
            @RequestParam(defaultValue = "HOUR") String interval,

            @Parameter(description = "Start timestamp (ISO-8601, inclusive).")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,

            @Parameter(description = "End timestamp (ISO-8601, inclusive).")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to
    ) {
        log.debug("GET /api/v1/analytics/timeseries requested [interval={}, from={}, to={}]", interval, from, to);
        TimeSeriesResponse timeSeries = analyticsService.getTimeSeries(interval, from, to);
        return ResponseEntity.ok(timeSeries);
    }

    @GetMapping("/users/{userId}")
    @Operation(
            summary = "Get individual user activity metrics",
            description = "Computes lifetime and recent summary metrics, active/total session counts, and event type breakdown for a user."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "User metrics successfully retrieved"),
            @ApiResponse(responseCode = "400", description = "User ID parameter invalid",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<UserAnalyticsResponse> getUserAnalytics(
            @Parameter(description = "Unique user identifier", required = true)
            @PathVariable String userId
    ) {
        log.debug("GET /api/v1/analytics/users/{} requested", userId);
        UserAnalyticsResponse userAnalytics = analyticsService.getUserAnalytics(userId);
        return ResponseEntity.ok(userAnalytics);
    }

    @GetMapping("/sessions/active")
    @Operation(
            summary = "List currently active user sessions",
            description = "Returns paginated active user sessions whose last seen timestamp is within the inactivity window (default: 30m)."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Active sessions retrieved")
    })
    public ResponseEntity<PageResponse<ActiveSessionResponse>> getActiveSessions(
            @Parameter(description = "Page number (0-indexed, default 0)")
            @RequestParam(defaultValue = "0") int page,

            @Parameter(description = "Page size (1-100, default 20)")
            @RequestParam(defaultValue = "20") int size
    ) {
        log.debug("GET /api/v1/analytics/sessions/active requested [page={}, size={}]", page, size);
        Page<ActiveSessionResponse> sessions = analyticsService.getActiveSessions(page, size);
        return ResponseEntity.ok(PageResponse.from(sessions));
    }
}
