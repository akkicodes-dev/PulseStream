package com.pulsestream.api.controller;

import com.pulsestream.api.dto.ErrorResponse;
import com.pulsestream.api.dto.EventRequest;
import com.pulsestream.api.dto.EventResponse;
import com.pulsestream.application.service.EventIngestionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API controller for ingesting user activity events into the streaming pipeline.
 */
@RestController
@RequestMapping(path = "/api/v1/events", produces = MediaType.APPLICATION_JSON_VALUE)
@Tag(name = "Event Ingestion", description = "High-throughput endpoint for streaming real-time user activity events")
@Slf4j
public class EventController {

    private final EventIngestionService eventIngestionService;

    public EventController(EventIngestionService eventIngestionService) {
        this.eventIngestionService = eventIngestionService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(
            summary = "Ingest user activity event",
            description = """
                    Accepts, validates, and ingests a user activity event into the streaming architecture.
                    
                    **Ingestion Workflow:**
                    1. Validates schema and required attributes (Jakarta Bean Validation).
                    2. Assigns a new UUID if `eventId` was omitted by the client.
                    3. Translates the request to a domain `ActivityEvent`.
                    4. Publishes to the Kafka topic `user.activity.events` partitioned by `userId`.
                    5. Returns `202 Accepted` only after Kafka brokers successfully acknowledge receipt (ISR commit).
                    
                    **Delivery & Reliability Guarantees:**
                    - At-least-once delivery with producer idempotence (`enable.idempotence=true`).
                    - Per-user chronological ordering guaranteed via `userId` partition key.
                    - Producer blocks synchronously up to the configured timeout (default: 5000ms).
                    
                    **Example curl Request:**
                    ```bash
                    curl -X POST http://localhost:8080/api/v1/events \\
                      -H "Content-Type: application/json" \\
                      -d '{
                        "userId": "usr_998877",
                        "sessionId": "sess_aabbcc1122",
                        "eventType": "PAGE_VIEW",
                        "timestamp": "2026-10-09T10:15:30Z",
                        "source": "WEB",
                        "metadata": {
                          "pageUrl": "/products/laptop-pro",
                          "referrer": "https://google.com"
                        }
                      }'
                    ```
                    """
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "202",
                    description = "Event accepted and successfully published to Kafka stream",
                    content = @Content(
                            mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = EventResponse.class),
                            examples = @ExampleObject(
                                    value = """
                                            {
                                              "eventId": "4f8a3d5e-2b1c-4e8f-9a3d-5e2b1c4e8f9a",
                                              "status": "ACCEPTED",
                                              "message": "Event accepted and published to stream",
                                              "timestamp": "2026-10-09T10:15:30.123Z"
                                            }
                                            """
                            )
                    )
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Bad Request: Validation failed, missing required fields, or malformed JSON",
                    content = @Content(
                            mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class),
                            examples = @ExampleObject(
                                    value = """
                                            {
                                              "timestamp": "2026-10-09T10:15:30.123Z",
                                              "status": 400,
                                              "error": "Bad Request",
                                              "message": "Validation failed for request payload",
                                              "path": "/api/v1/events",
                                              "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                                              "details": [
                                                "userId: userId is required and cannot be blank"
                                              ]
                                            }
                                            """
                            )
                    )
            ),
            @ApiResponse(
                    responseCode = "503",
                    description = "Service Unavailable: Kafka broker unreachable or publication timed out",
                    content = @Content(
                            mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class),
                            examples = @ExampleObject(
                                    value = """
                                            {
                                              "timestamp": "2026-10-09T10:15:30.123Z",
                                              "status": 503,
                                              "error": "Service Unavailable",
                                              "message": "Event streaming service temporarily unavailable: Timed out waiting for Kafka broker acknowledgment after 5000ms",
                                              "path": "/api/v1/events",
                                              "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
                                            }
                                            """
                            )
                    )
            )
    })
    public ResponseEntity<EventResponse> ingestEvent(@Valid @RequestBody EventRequest request) {
        EventResponse response = eventIngestionService.ingestEvent(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
