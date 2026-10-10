# PulseStream: Milestone 3 Manual & Automated Verification

## 1. Overview & Scope

Milestone 3 implements the asynchronous consumer pipeline:
1. **Kafka Consumer (`ActivityEventConsumer`)**: Subscribes to topic `user.activity.events` with consumer group `ps-activity-consumer`.
2. **PostgreSQL Persistence Service (`ActivityEventPersistenceService`)**: Idempotently persists valid `ActivityEvent` instances into the `activity_events` table using existing entities, repositories, and constraints.
3. **Robust Error Handling & DLQ (`KafkaConsumerConfig`)**: Employs `ErrorHandlingDeserializer` for poison pill isolation, `DefaultErrorHandler` for bounded retries (3 total attempts), `DeadLetterPublishingRecoverer` for dead-letter queuing to `user.activity.dlq`, and `AckMode.RECORD` for offset commit synchronization after database transactions.

---

## 2. Automated Test Results (`.\mvnw.cmd test`)

Full automated test suite executed with all 27 unit and integration tests passing:

```text
[INFO] Results:
[INFO] 
[INFO] Tests run: 27, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  27.868 s
```

### Test Breakdown by Component
| Test Class | Category | Scenarios Covered | Status |
|---|---|---|---|
| `ActivityEventPersistenceServiceTest` | Unit (Mockito) | New event persistence, application-level duplicate detection, concurrent `DataIntegrityViolationException` race handling, DB failure propagation, validation | **PASS** (6/6) |
| `ActivityEventConsumerTest` | Unit (Mockito) | Record consumption delegation, null payload safety, persistence exception propagation | **PASS** (3/3) |
| `ActivityEventConsumerIntegrationTest` | Integration (`@EmbeddedKafka`) | Full end-to-end consumer-to-service flow, malformed JSON poison pill routing directly to `user.activity.dlq`, DB failure retry and DLQ routing | **PASS** (3/3) |
| `EventControllerTest` | Unit | Milestone 2 REST controller contract and validation tests | **PASS** (5/5) |
| `EventIngestionApiIntegrationTest` | Integration | Milestone 2 HTTP ingestion to Kafka integration | **PASS** (4/4) |
| `EventIngestionServiceTest` | Unit | Milestone 2 Ingestion service unit validation | **PASS** (3/3) |
| `KafkaEventProducerTest` | Unit | Milestone 2 Producer unit tests | **PASS** (2/2) |
| `KafkaEventProducerIntegrationTest` | Integration | Milestone 2 Embedded Kafka producer delivery and partition tests | **PASS** (1/1) |

---

## 3. Live Infrastructure End-to-End Verification

Live verification executed against real Docker containers:
- **PostgreSQL**: `localhost:5433` (container: `pulsestream-postgres`)
- **Apache Kafka**: `localhost:9092` (container: `pulsestream-kafka`)
- **Kafka UI**: `http://localhost:8090` (container: `pulsestream-kafka-ui`)
- **Spring Boot Application**: `http://localhost:8081`

### A. Live End-to-End Pipeline (HTTP -> Kafka -> Consumer -> PostgreSQL)
1. **HTTP Ingestion Request**:
   ```bash
   curl -i -X POST http://localhost:8081/api/v1/events \
     -H "Content-Type: application/json" \
     -d '{
       "userId":"usr_m3_live_test",
       "sessionId":"sess_m3_001",
       "eventType":"PURCHASE",
       "timestamp":"2026-10-10T12:25:00Z",
       "source":"MOBILE",
       "metadata":{"orderId":"ord_999","amount":149.99}
     }'
   ```
   - **Response**: `HTTP 202 Accepted`
   - **Assigned eventId**: `b995d359-95cc-49ae-a41d-ff63925b71fe`

2. **Consumer Observation in Application Logs**:
   ```text
   Consuming ActivityEvent from Kafka [topic=user.activity.events, partition=1, offset=0, key=usr_m3_live_test]: eventId=b995d359-95cc-49ae-a41d-ff63925b71fe, type=PURCHASE
   Successfully persisted ActivityEvent to PostgreSQL: eventId=b995d359-95cc-49ae-a41d-ff63925b71fe
   ```

3. **PostgreSQL Database Verification**:
   ```sql
   SELECT event_id, user_id, event_type, source, metadata FROM activity_events WHERE user_id = 'usr_m3_live_test';
   ```
   **Observed Result**:
   ```text
                  event_id               |     user_id      | event_type | source |                 metadata                 
   --------------------------------------+------------------+------------+--------+------------------------------------------
    b995d359-95cc-49ae-a41d-ff63925b71fe | usr_m3_live_test | PURCHASE   | mobile | {"amount": 149.99, "orderId": "ord_999"}
   (1 row)
   ```
   - **Status**: **PASS**

### B. Live Idempotency & Duplicate Delivery Guard
1. **Duplicate Request Sent**: Sent duplicate payload with the identical `eventId` (`b995d359-95cc-49ae-a41d-ff63925b71fe`).
2. **Observed Consumer Log**:
   ```text
   INFO c.p.a.s.ActivityEventPersistenceService - Duplicate event detected (eventId=b995d359-95cc-49ae-a41d-ff63925b71fe). Skipping insertion to preserve idempotency.
   ```
3. **Database Row Count Verification**:
   ```sql
   SELECT count(*) FROM activity_events WHERE event_id = 'b995d359-95cc-49ae-a41d-ff63925b71fe';
   ```
   **Observed Result**: `1` (No duplicate rows inserted).
   - **Status**: **PASS**

---

## 4. Verification Check Matrix

| Check ID | Verification Item | Target | Observed Result | Status |
|---|---|---|---|---|
| M3-01 | Kafka Topic Consumption | `user.activity.events` | Consumer successfully subscribes and reads partitions | **PASS** |
| M3-02 | PostgreSQL Persistence | Table `activity_events` | Entity correctly mapped and stored with JSONB metadata | **PASS** |
| M3-03 | Application Idempotency | `existsByEventId` | Duplicate event detected and skipped | **PASS** |
| M3-04 | Database Constraint Guard | `uq_activity_events_event_id` | `DataIntegrityViolationException` caught and handled safely | **PASS** |
| M3-05 | Transactional Synchronization | `AckMode.RECORD` | Offset committed only after DB transaction commits | **PASS** |
| M3-06 | Poison Pill Isolation | `ErrorHandlingDeserializer` | Malformed payloads do not stop consumer thread | **PASS** |
| M3-07 | Dead-Letter Queue (DLQ) | `user.activity.dlq` | Poison pills and exhausted retries routed to DLQ | **PASS** |
| M3-08 | Transient Retry Strategy | `DefaultErrorHandler` | Transient DB failures retried with bounded backoff | **PASS** |
| M3-09 | M1/M2 Regression | Full test suite | All 15 existing tests continue to pass | **PASS** |
| M3-10 | Full Project Build | `mvnw test` | 27/27 tests pass with `BUILD SUCCESS` | **PASS** |
