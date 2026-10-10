package com.pulsestream.api.exception;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.pulsestream.api.dto.ErrorResponse;
import com.pulsestream.api.filter.RequestIdFilter;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.exception.EventPublishingException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.pulsestream.infrastructure.metrics.PulseStreamMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;

/**
 * Centralized exception handler providing consistent, RFC-7807-inspired error responses.
 *
 * <p>Never exposes raw exception stack traces to API clients. Correlates all errors
 * with an {@code X-Request-Id} for end-to-end tracing.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    private final PulseStreamMetrics metrics;

    public GlobalExceptionHandler() {
        this(null);
    }

    @Autowired
    public GlobalExceptionHandler(@Nullable PulseStreamMetrics metrics) {
        this.metrics = metrics;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpServletRequest request) {

        if (metrics != null) {
            metrics.recordEventRejected("validation_error");
        }

        List<String> details = new ArrayList<>();
        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            details.add(fieldError.getField() + ": " + fieldError.getDefaultMessage());
        }

        String path = request.getRequestURI();
        String reqId = resolveRequestId(request);

        log.warn("Validation error on path {}: {}", path, details);

        ErrorResponse errorResponse = new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                "Validation failed for request payload",
                path,
                reqId,
                details
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleHttpMessageNotReadable(
            HttpMessageNotReadableException ex, HttpServletRequest request) {

        if (metrics != null) {
            metrics.recordEventRejected("malformed_json");
        }

        String path = request.getRequestURI();
        String reqId = resolveRequestId(request);
        String message = "Malformed JSON request payload";

        Throwable cause = ex.getCause();
        if (cause instanceof InvalidFormatException ife) {
            Class<?> targetType = ife.getTargetType();
            if (targetType != null && targetType.isEnum()) {
                if (EventType.class.isAssignableFrom(targetType)) {
                    message = "Invalid eventType: '" + ife.getValue() + "'. Allowed values: "
                            + Arrays.toString(EventType.values());
                } else if (EventSource.class.isAssignableFrom(targetType)) {
                    message = "Invalid source: '" + ife.getValue() + "'. Allowed values: "
                            + Arrays.toString(EventSource.values());
                } else {
                    message = "Invalid enum value '" + ife.getValue() + "' for type " + targetType.getSimpleName();
                }
            } else if (Instant.class.isAssignableFrom(targetType)) {
                message = "Invalid timestamp format: '" + ife.getValue() + "'. Expected valid ISO-8601 string (e.g. 2026-10-09T10:15:30Z)";
            } else {
                message = "Invalid format for field value: '" + ife.getValue() + "'";
            }
        } else if (cause instanceof JsonParseException jpe) {
            message = "Malformed JSON syntax: " + jpe.getOriginalMessage();
        } else if (cause instanceof MismatchedInputException mie) {
            message = "Malformed JSON payload structure: " + mie.getOriginalMessage();
        } else if (cause != null && cause.getMessage() != null) {
            message = "Invalid request body: " + cause.getMessage();
        }

        log.warn("Malformed or unreadable HTTP request on path {}: {}", path, message);

        ErrorResponse errorResponse = new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                message,
                path,
                reqId,
                null
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(
            ConstraintViolationException ex, HttpServletRequest request) {

        if (metrics != null) {
            metrics.recordEventRejected("constraint_violation");
        }

        List<String> details = ex.getConstraintViolations().stream()
                .map(cv -> cv.getPropertyPath() + ": " + cv.getMessage())
                .toList();

        String path = request.getRequestURI();
        String reqId = resolveRequestId(request);

        log.warn("Constraint violation on path {}: {}", path, details);

        ErrorResponse errorResponse = new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                "Constraint violation in request parameters",
                path,
                reqId,
                details
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgumentException(
            IllegalArgumentException ex, HttpServletRequest request) {

        if (metrics != null) {
            metrics.recordEventRejected("illegal_argument");
        }

        String path = request.getRequestURI();
        String reqId = resolveRequestId(request);

        log.warn("Invalid argument on path {}: {}", path, ex.getMessage());

        ErrorResponse errorResponse = new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                ex.getMessage(),
                path,
                reqId,
                null
        );

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(EventPublishingException.class)
    public ResponseEntity<ErrorResponse> handleEventPublishingException(
            EventPublishingException ex, HttpServletRequest request) {

        if (metrics != null) {
            metrics.recordEventRejected("kafka_unavailable");
        }

        String path = request.getRequestURI();
        String reqId = resolveRequestId(request);

        log.error("Kafka publication failure on path {}: {}", path, ex.getMessage());

        ErrorResponse errorResponse = new ErrorResponse(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                "Event streaming service temporarily unavailable: " + ex.getMessage(),
                path,
                reqId,
                null
        );

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(errorResponse);
    }

    @ExceptionHandler(org.springframework.web.servlet.resource.NoResourceFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoResourceFound(
            org.springframework.web.servlet.resource.NoResourceFoundException ex, HttpServletRequest request) {

        String path = request.getRequestURI();
        String reqId = resolveRequestId(request);

        ErrorResponse errorResponse = new ErrorResponse(
                HttpStatus.NOT_FOUND.value(),
                HttpStatus.NOT_FOUND.getReasonPhrase(),
                "Resource not found: " + path,
                path,
                reqId,
                null
        );

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
    }

    @ExceptionHandler(org.springframework.web.HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethodNotSupported(
            org.springframework.web.HttpRequestMethodNotSupportedException ex, HttpServletRequest request) {

        String path = request.getRequestURI();
        String reqId = resolveRequestId(request);

        ErrorResponse errorResponse = new ErrorResponse(
                HttpStatus.METHOD_NOT_ALLOWED.value(),
                HttpStatus.METHOD_NOT_ALLOWED.getReasonPhrase(),
                ex.getMessage(),
                path,
                reqId,
                null
        );

        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(errorResponse);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(
            Exception ex, HttpServletRequest request) {

        String path = request.getRequestURI();
        String reqId = resolveRequestId(request);

        log.error("Unhandled internal server error on path {} [requestId={}]:", path, reqId, ex);

        ErrorResponse errorResponse = new ErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                "An unexpected internal error occurred while processing the request",
                path,
                reqId,
                null
        );

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    private String resolveRequestId(HttpServletRequest request) {
        String reqId = MDC.get(RequestIdFilter.MDC_KEY);
        if (reqId == null || reqId.isBlank()) {
            reqId = request.getHeader(RequestIdFilter.REQUEST_ID_HEADER);
        }
        return reqId;
    }
}
