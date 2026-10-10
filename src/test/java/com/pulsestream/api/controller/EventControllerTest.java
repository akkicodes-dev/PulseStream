package com.pulsestream.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulsestream.api.dto.EventRequest;
import com.pulsestream.api.dto.EventResponse;
import com.pulsestream.api.exception.GlobalExceptionHandler;
import com.pulsestream.api.filter.RequestIdFilter;
import com.pulsestream.application.service.EventIngestionService;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.exception.EventPublishingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class EventControllerTest {

    @Mock
    private EventIngestionService eventIngestionService;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();

        mockMvc = MockMvcBuilders.standaloneSetup(new EventController(eventIngestionService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .addFilters(new RequestIdFilter())
                .setValidator(validator)
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    @DisplayName("1 & 6. Valid event request returns 202 Accepted with assigned eventId")
    void validEventRequest_returns202Accepted() throws Exception {
        String generatedId = UUID.randomUUID().toString();
        when(eventIngestionService.ingestEvent(any(EventRequest.class)))
                .thenReturn(EventResponse.accepted(generatedId));

        String requestJson = """
                {
                    "userId": "usr_12345",
                    "sessionId": "sess_abcdef",
                    "eventType": "PAGE_VIEW",
                    "timestamp": "2026-10-09T10:15:30Z",
                    "source": "WEB",
                    "metadata": {
                        "pageUrl": "/home"
                    }
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("X-Request-Id"))
                .andExpect(jsonPath("$.eventId").value(generatedId))
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.message").value("Event accepted and published to stream"))
                .andExpect(jsonPath("$.timestamp").isNotEmpty());

        verify(eventIngestionService).ingestEvent(any(EventRequest.class));
    }

    @Test
    @DisplayName("2. Missing required fields returns 400 Bad Request with validation details")
    void missingRequiredFields_returns400BadRequest() throws Exception {
        // Missing userId, sessionId, eventType, timestamp, source
        String requestJson = """
                {
                    "metadata": {}
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(header().exists("X-Request-Id"))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value("Validation failed for request payload"))
                .andExpect(jsonPath("$.path").value("/api/v1/events"))
                .andExpect(jsonPath("$.requestId").isNotEmpty())
                .andExpect(jsonPath("$.details").isArray());
    }

    @Test
    @DisplayName("3. Invalid event type returns 400 Bad Request with informative error")
    void invalidEventType_returns400BadRequest() throws Exception {
        String requestJson = """
                {
                    "userId": "usr_123",
                    "sessionId": "sess_456",
                    "eventType": "UNKNOWN_ACTION",
                    "timestamp": "2026-10-09T10:15:30Z",
                    "source": "WEB"
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message", containsString("Invalid eventType")))
                .andExpect(jsonPath("$.path").value("/api/v1/events"))
                .andExpect(jsonPath("$.requestId").isNotEmpty());
    }

    @Test
    @DisplayName("4. Missing event ID allows UUID generation and returns 202 Accepted")
    void missingEventId_generatesUuidAndReturns202() throws Exception {
        String generatedUuid = UUID.randomUUID().toString();
        when(eventIngestionService.ingestEvent(any(EventRequest.class)))
                .thenReturn(EventResponse.accepted(generatedUuid));

        // eventId field omitted intentionally
        String requestJson = """
                {
                    "userId": "usr_789",
                    "sessionId": "sess_012",
                    "eventType": "LOGIN",
                    "timestamp": "2026-10-09T10:15:30Z",
                    "source": "MOBILE"
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(generatedUuid))
                .andExpect(jsonPath("$.status").value("ACCEPTED"));
    }

    @Test
    @DisplayName("7. Kafka publication failure does NOT return 202 (returns 503 Service Unavailable)")
    void kafkaPublicationFailure_returns503ServiceUnavailable() throws Exception {
        when(eventIngestionService.ingestEvent(any(EventRequest.class)))
                .thenThrow(new EventPublishingException("Timed out waiting for Kafka broker acknowledgment after 5000ms"));

        String requestJson = """
                {
                    "userId": "usr_fail",
                    "sessionId": "sess_fail",
                    "eventType": "PURCHASE",
                    "timestamp": "2026-10-09T10:15:30Z",
                    "source": "API"
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.error").value("Service Unavailable"))
                .andExpect(jsonPath("$.message", containsString("Event streaming service temporarily unavailable")))
                .andExpect(jsonPath("$.path").value("/api/v1/events"))
                .andExpect(jsonPath("$.requestId").isNotEmpty());
    }

    @Test
    @DisplayName("8. Malformed JSON returns 400 Bad Request")
    void malformedJson_returns400BadRequest() throws Exception {
        String malformedJson = "{ \"userId\": \"usr_1\", \"sessionId\": ";

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(malformedJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message", containsString("Malformed JSON")))
                .andExpect(jsonPath("$.path").value("/api/v1/events"))
                .andExpect(jsonPath("$.requestId").isNotEmpty());
    }

    @Test
    @DisplayName("9. Consistent error-response format verifies all standardized fields")
    void consistentErrorResponseFormat_verifiedAcrossAllErrors() throws Exception {
        // Test invalid UUID format for eventId
        String requestJson = """
                {
                    "eventId": "not-a-valid-uuid",
                    "userId": "usr_valid",
                    "sessionId": "sess_valid",
                    "eventType": "LOGIN",
                    "timestamp": "2026-10-09T10:15:30Z",
                    "source": "WEB"
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .header("X-Request-Id", "custom-trace-id-1234")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("X-Request-Id", "custom-trace-id-1234"))
                .andExpect(jsonPath("$.timestamp", notNullValue()))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value("Validation failed for request payload"))
                .andExpect(jsonPath("$.path").value("/api/v1/events"))
                .andExpect(jsonPath("$.requestId").value("custom-trace-id-1234"))
                .andExpect(jsonPath("$.details").isArray());
    }
}
