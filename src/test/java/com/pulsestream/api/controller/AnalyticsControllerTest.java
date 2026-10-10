package com.pulsestream.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulsestream.api.dto.response.*;
import com.pulsestream.api.exception.GlobalExceptionHandler;
import com.pulsestream.api.filter.RequestIdFilter;
import com.pulsestream.application.service.AnalyticsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.web.PageableHandlerMethodArgumentResolver;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class AnalyticsControllerTest {

    @Mock
    private AnalyticsService analyticsService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        mockMvc = MockMvcBuilders.standaloneSetup(new AnalyticsController(analyticsService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .setCustomArgumentResolvers(new PageableHandlerMethodArgumentResolver())
                .addFilters(new RequestIdFilter())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    @DisplayName("GET /api/v1/analytics/overview should return 200 with overview metrics")
    void shouldReturnOverviewMetrics() throws Exception {
        OffsetDateTime from = OffsetDateTime.parse("2026-10-10T00:00:00Z");
        OffsetDateTime to = OffsetDateTime.parse("2026-10-10T12:00:00Z");

        AnalyticsOverviewResponse mockResponse = new AnalyticsOverviewResponse(
                from,
                to,
                150L,
                42L,
                10L,
                Map.of("PAGE_VIEW", 100L, "LOGIN", 50L),
                Map.of("web", 120L, "mobile", 30L)
        );

        when(analyticsService.getOverview(any(), any())).thenReturn(mockResponse);

        mockMvc.perform(get("/api/v1/analytics/overview")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalEvents", is(150)))
                .andExpect(jsonPath("$.uniqueUsers", is(42)))
                .andExpect(jsonPath("$.activeSessions", is(10)))
                .andExpect(jsonPath("$.eventsByType.PAGE_VIEW", is(100)))
                .andExpect(jsonPath("$.eventsBySource.web", is(120)));
    }

    @Test
    @DisplayName("GET /api/v1/analytics/timeseries should return 200 with buckets")
    void shouldReturnTimeSeriesBuckets() throws Exception {
        OffsetDateTime from = OffsetDateTime.parse("2026-10-10T10:00:00Z");
        OffsetDateTime to = OffsetDateTime.parse("2026-10-10T12:00:00Z");

        TimeSeriesResponse mockResponse = new TimeSeriesResponse(
                "HOUR",
                from,
                to,
                2,
                List.of(
                        new TimeSeriesBucket(from, 15L, 5L),
                        new TimeSeriesBucket(to, 25L, 8L)
                )
        );

        when(analyticsService.getTimeSeries(eq("HOUR"), any(), any())).thenReturn(mockResponse);

        mockMvc.perform(get("/api/v1/analytics/timeseries")
                        .param("interval", "HOUR")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.interval", is("HOUR")))
                .andExpect(jsonPath("$.bucketCount", is(2)))
                .andExpect(jsonPath("$.buckets", hasSize(2)))
                .andExpect(jsonPath("$.buckets[0].totalEvents", is(15)))
                .andExpect(jsonPath("$.buckets[1].totalEvents", is(25)));
    }

    @Test
    @DisplayName("GET /api/v1/analytics/timeseries should return 400 when interval is rejected by allowlist")
    void shouldReturn400OnInvalidInterval() throws Exception {
        when(analyticsService.getTimeSeries(eq("INVALID"), any(), any()))
                .thenThrow(new IllegalArgumentException("Invalid interval: 'INVALID'. Allowed intervals are: MINUTE, HOUR, DAY"));

        mockMvc.perform(get("/api/v1/analytics/timeseries")
                        .param("interval", "INVALID")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("Invalid interval: 'INVALID'. Allowed intervals are: MINUTE, HOUR, DAY")));
    }

    @Test
    @DisplayName("GET /api/v1/analytics/users/{userId} should return 200 with user analytics")
    void shouldReturnUserAnalytics() throws Exception {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-10T12:00:00Z");
        UserAnalyticsResponse mockResponse = new UserAnalyticsResponse(
                "usr-789",
                50L,
                4L,
                1L,
                now.minusDays(5),
                now,
                Map.of("PAGE_VIEW", 35L, "PURCHASE", 15L)
        );

        when(analyticsService.getUserAnalytics("usr-789")).thenReturn(mockResponse);

        mockMvc.perform(get("/api/v1/analytics/users/usr-789")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userId", is("usr-789")))
                .andExpect(jsonPath("$.totalEvents", is(50)))
                .andExpect(jsonPath("$.totalSessions", is(4)))
                .andExpect(jsonPath("$.activeSessions", is(1)))
                .andExpect(jsonPath("$.eventsByType.PURCHASE", is(15)));
    }

    @Test
    @DisplayName("GET /api/v1/analytics/sessions/active should return 200 with paginated active sessions")
    void shouldReturnActiveSessions() throws Exception {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-10T12:00:00Z");
        ActiveSessionResponse session = new ActiveSessionResponse(
                "sess-1",
                "usr-1",
                now.minusMinutes(10),
                now.minusMinutes(1),
                5,
                true
        );

        when(analyticsService.getActiveSessions(0, 20))
                .thenReturn(new PageImpl<>(List.of(session)));

        mockMvc.perform(get("/api/v1/analytics/sessions/active")
                        .param("page", "0")
                        .param("size", "20")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(1)))
                .andExpect(jsonPath("$.content[0].sessionId", is("sess-1")))
                .andExpect(jsonPath("$.content[0].active", is(true)));
    }
}
