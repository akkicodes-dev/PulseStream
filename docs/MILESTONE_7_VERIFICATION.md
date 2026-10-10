# PulseStream: Milestone 7 Verification Report

## Backend Observability, Health, and Reliability Metrics

### 1. Executive Summary

Milestone 7 implements end-to-end operational observability, health probing, failure recovery, and metric instrumentation for the PulseStream platform:
1. **Actuator Health & Probes**:
   - Enabled Kubernetes-ready **liveness** (`/actuator/health/liveness`) and **readiness** (`/actuator/health/readiness`) probes.
   - Readiness probes actively verify PostgreSQL database connectivity (`health.db.enabled: true`).
   - Explicitly disabled phantom health checks (`redis`, `elasticsearch`) to eliminate false-negative degradation alarms.
2. **Security & Endpoint Hardening**:
   - Web exposure restricted to `health, info, metrics, prometheus`.
   - Explicitly eliminated `/actuator/env` and `/actuator/loggers` from web exposure, preventing credential, token, or configuration leakage.
   - Added `NoResourceFoundException` and `HttpRequestMethodNotSupportedException` handlers in `GlobalExceptionHandler` to prevent 404/405 client errors from inflating 500 internal server error alarms.
3. **Metric Instrumentation (`PulseStreamMetrics`)**:
   - Introduced `io.micrometer:micrometer-registry-prometheus` for Prometheus scraping (`/actuator/prometheus`).
   - Centralized, bounded metric instrumentation covering ingestion, persistence, duplicate deduplication, consumer processing, DLQ routing, session lifecycle, and analytics latency.
   - **Zero High-Cardinality Labels**: Strictly banned dynamic identifiers (`userId`, `sessionId`, `eventId`, URLs) from tags, guaranteeing O(1) memory footprint in metric registries.

---

## 2. Metric Catalog & Bounded Cardinality

All metrics are exposed via `/actuator/metrics` and scraped via `/actuator/prometheus`:

| Metric Name | Type | Description | Allowed Tags (Bounded) |
| :--- | :--- | :--- | :--- |
| `pulsestream.events.accepted` | Counter | Total activity events accepted by ingestion API | `event_type`, `source` |
| `pulsestream.events.rejected` | Counter | Ingestion requests rejected at boundary | `reason` (`validation_error`, `malformed_json`, `kafka_unavailable`, `constraint_violation`, `illegal_argument`) |
| `pulsestream.events.persisted` | Counter | Valid events successfully committed to DB | `event_type`, `source` |
| `pulsestream.events.duplicate` | Counter | Duplicate deliveries skipped idempotently | `source` |
| `pulsestream.events.persistence.failures` | Counter | Database write failures | `reason` (`db_error`) |
| `pulsestream.consumer.failures` | Counter | Consumer execution errors | `reason` (`persistence_error`) |
| `pulsestream.events.dlq` | Counter | Messages routed to dead-letter queue | `dlq_topic`, `reason` (`deserialization_exception`, `illegal_argument_exception`) |
| `pulsestream.sessions.created` | Counter | New user sessions established | `status` (`created`) |
| `pulsestream.sessions.closed` | Counter | Sessions closed | `reason` (`logout`, `inactivity_sweep`) |
| `pulsestream.analytics.requests` | Timer | Request duration and volume for analytics | `endpoint` (`overview`, `timeseries`, `user_analytics`, `active_sessions`), `status` (`success`, `error`) |

---

## 3. Failure Recovery & Error Invariants

1. **Poison Pill Isolation**:
   - `ErrorHandlingDeserializer` intercepts unparseable payloads before the consumer thread crashes.
   - Marked non-retryable in `DefaultErrorHandler`; routed directly to `user.activity.dlq` via `DeadLetterPublishingRecoverer` with diagnostic exception headers.
   - Offset committed, partition consumption continues unblocked.
2. **Transient Outage Resilience**:
   - Database connection blips trigger bounded retries (2 attempts, 1000ms fixed backoff).
   - If retries exhaust, the message is routed to DLQ and the failure is recorded.
3. **Correlation & Log Sanitization**:
   - `RequestIdFilter` propagates `X-Request-Id` across all requests into SLF4J MDC (`requestId`).
   - Stack traces and sensitive metadata payloads are never exposed to external HTTP callers.
   - RFC-7807 standardized error payloads are returned for all 4xx and 5xx errors.

---

## 4. Verification Evidence & Automated Tests

Full Maven test suite execution confirms **67 tests passing with 0 failures**:
- `PulseStreamMetricsTest` (8 tests):
  - Counter increments for accepted, rejected, persisted, duplicate, failure, and DLQ events.
  - Tag sanitization for null, blank, and mixed-case values.
  - Timer recording for successful queries and error outcomes.
- `ActuatorObservabilityIntegrationTest` (7 tests):
  - Verified `/actuator/health` returns HTTP 200 with `status: UP`.
  - Verified `/actuator/health/liveness` returns HTTP 200 with `status: UP`.
  - Verified `/actuator/health/readiness` returns HTTP 200 with `status: UP` and database connection check.
  - Verified `/actuator/metrics` lists `pulsestream.events.accepted` and `pulsestream.events.persisted`.
  - Verified `/actuator/metrics/pulsestream.events.accepted` returns tag values (`event_type: purchase`, etc.).
  - Verified `/actuator/prometheus` returns HTTP 200 text format containing `pulsestream_events_accepted_total`.
  - Verified `/actuator/env` returns HTTP 404 (strictly blocked and unexposed).

```text
[INFO] Results:
[INFO] 
[INFO] Tests run: 67, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

---

## 5. Operational Inspection Commands

### Health & Readiness
```bash
# General health
curl -s http://localhost:8080/actuator/health

# Kubernetes Liveness Probe
curl -s http://localhost:8080/actuator/health/liveness

# Kubernetes Readiness Probe (verifies PostgreSQL connection)
curl -s http://localhost:8080/actuator/health/readiness
```

### Metrics & Prometheus Scraping
```bash
# List all registered metric names
curl -s http://localhost:8080/actuator/metrics

# Query accepted event count
curl -s http://localhost:8080/actuator/metrics/pulsestream.events.accepted

# Query rejected event count
curl -s http://localhost:8080/actuator/metrics/pulsestream.events.rejected

# Scrape Prometheus exposition format
curl -s http://localhost:8080/actuator/prometheus
```

### Dead-Letter Queue Inspection
```bash
# Inspect messages routed to user.activity.dlq
docker exec -it pulsestream-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user.activity.dlq \
  --from-beginning \
  --property print.headers=true \
  --property print.key=true
```
