# PulseStream: Milestone 4 Verification Report

## 1. Overview & Scope

Milestone 4 implements real-time activity analytics and session aggregation entirely within the existing PostgreSQL 16 and Kafka 3.7 architecture (with zero new infrastructure or external containers):
1. **Database Migration (`V4__add_analytics_indexes.sql`)**: Compound indexes for time-window aggregations (`idx_ae_ts_type`, `idx_ae_ts_source`) and partial active session index (`idx_us_active_last_seen`).
2. **Session Aggregation Service (`UserSessionAggregationService`)**:
   - Atomic event-session lifecycle management (`recordEventInSession`).
   - Closed-session safeguards: sessions closed by `LOGOUT` or expiration are never silently reopened; late-arriving events increment `event_count` and adjust `last_seen_at` while preserving `isActive = false` and keeping `ended_at` intact.
   - Background inactivity expiration sweep: `@Scheduled` periodic deactivation of idle sessions older than 30 minutes.
3. **Transaction-Safe Persistence (`ActivityEventPersistenceService`)**:
   - Native `ON CONFLICT (event_id) DO NOTHING` via `JdbcTemplate` to eliminate JPA Hibernate rollback-only transaction poisoning.
   - Session aggregation triggers strictly when `affectedRows == 1` (genuinely new insert).
   - Atomic multi-table consistency across event persistence and session aggregation.
4. **Bounded Analytics Service (`AnalyticsService`)**:
   - Strict interval allowlist (`MINUTE`, `HOUR`, `DAY`) with max range limits (6h, 14d, 90d) and 500 bucket hard ceiling.
   - Static parameterized native queries with zero dynamic SQL concatenation.
   - Continuous UTC gap-filling ensuring zero-filled buckets for quiet intervals.
5. **REST Analytics Endpoints (`AnalyticsController`)**:
   - `GET /api/v1/analytics/overview`
   - `GET /api/v1/analytics/timeseries`
   - `GET /api/v1/analytics/users/{userId}`
   - `GET /api/v1/analytics/sessions/active`

---

## 2. Automated Test Results (`.\mvnw.cmd test`)

All 46 unit and integration tests passed with 0 failures and 0 errors:

```text
[INFO] Results:
[INFO] 
[INFO] Tests run: 46, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  29.108 s
```

### Test Breakdown by Component
| Test Class | Category | Scenarios Covered | Status |
|---|---|---|---|
| `UserSessionAggregationServiceTest` | Unit (Mockito) | New active session creation, pre-closed session on LOGOUT, active session update, LOGOUT closure, **closed-session non-reopening invariant**, expired session late event safety, out-of-order `started_at` adjustment, scheduled inactivity sweep | **PASS** (7/7) |
| `ActivityEventPersistenceServiceTest` | Unit (Mockito) | New event persistence with session aggregation (`affectedRows=1`), **transaction-safe duplicate handling via ON CONFLICT without session re-aggregation (`affectedRows=0`)**, atomic rollback on session error, transient failure propagation | **PASS** (6/6) |
| `AnalyticsServiceTest` | Unit (Mockito) | Overview metric calculations, `from > to` rejection, interval allowlist enforcement (`MINUTE`, `HOUR`, `DAY`), max time range bounds, **continuous UTC gap-filling**, user lifetime metrics | **PASS** (6/6) |
| `AnalyticsControllerTest` | Unit (MockMvc) | `GET /overview` JSON contracts, `GET /timeseries` gap-filled response, interval validation 400 rejection, `GET /users/{userId}`, `GET /sessions/active` pagination | **PASS** (5/5) |
| `ActivityEventConsumerTest` | Unit | Consumer delegation, null payload safety, persistence error propagation | **PASS** (3/3) |
| `ActivityEventConsumerIntegrationTest` | Integration | Kafka consumer to service flow, poison pill DLQ isolation, retry semantics | **PASS** (3/3) |
| `EventControllerTest` | Unit | Event ingestion HTTP contracts, request validation, 202 Accepted | **PASS** (5/5) |
| `EventIngestionApiIntegrationTest` | Integration | End-to-end HTTP ingestion through embedded Kafka pipeline | **PASS** (4/4) |
| `EventIngestionServiceTest` | Unit | Domain model transformation, UUID generation, producer delegation | **PASS** (3/3) |
| `KafkaEventProducerTest` | Unit | Synchronous acknowledgment, timeout, broker error mapping | **PASS** (2/2) |
| `KafkaEventProducerIntegrationTest` | Integration | Embedded Kafka topic delivery, partition key routing | **PASS** (1/1) |
| `PulseStreamApplicationTests` | Smoke | Spring context initialization smoke test | **PASS** (1/1) |

---

## 3. Live End-to-End Docker Verification

Live end-to-end verification executed against active Docker services:
- **PostgreSQL**: `localhost:5433` (`pulsestream-postgres`)
- **Apache Kafka**: `localhost:9092` (`pulsestream-kafka`)
- **Spring Boot Application**: `http://localhost:8081`

### Step-by-Step Live Execution Results

#### Step 1: Ingest Initial Session Event (`LOGIN`)
- Payload: `userId=usr_m4_live`, `sessionId=sess_m4_live_1`, `eventType=LOGIN`, `timestamp=2026-10-10T12:00:00Z`
- Response: `202 ACCEPTED`
- Log: `Created active session: sessionId=sess_m4_live_1, userId=usr_m4_live`

#### Step 2: Ingest Subsequent Event (`PAGE_VIEW`)
- Payload: Same session `sess_m4_live_1`, `eventType=PAGE_VIEW`, `timestamp=2026-10-10T12:05:00Z`
- Response: `202 ACCEPTED`
- Log: `Successfully persisted new ActivityEvent: eventId=e97d6877-45b5-4b32-b280-3b16e7e33bbd`

#### Step 3: Concurrency & Duplicate Invariant Verification
- Payload: Resent exact duplicate `eventId=e97d6877-45b5-4b32-b280-3b16e7e33bbd`
- Response: `202 ACCEPTED`
- Log: `Duplicate event detected (eventId=e97d6877-45b5-4b32-b280-3b16e7e33bbd). Conflict handled safely without session re-aggregation.`
- Result: **Transaction completed safely, `event_count` not double-incremented.**

#### Step 4: Explicit Session Closure (`LOGOUT`)
- Payload: Same session `sess_m4_live_1`, `eventType=LOGOUT`, `timestamp=2026-10-10T12:15:00Z`
- Response: `202 ACCEPTED`
- Log: `Closed active session on LOGOUT: sessionId=sess_m4_live_1, userId=usr_m4_live`

#### Step 5: Closed-Session Invariant Verification (Late Event)
- Payload: Sent late event `PRODUCT_VIEW` with timestamp `2026-10-10T12:20:00Z` to closed session `sess_m4_live_1`
- Response: `202 ACCEPTED`
- Log: `Late event f295f18b-c343-42c1-8a92-e48e5f27334b arrived for closed session sess_m4_live_1. Incremented eventCount (4) without reopening session.`

#### Step 6: Database State Inspection in PostgreSQL (`user_sessions`)
Direct Docker SQL query (`SELECT session_id, user_id, started_at, last_seen_at, ended_at, event_count, is_active FROM user_sessions`):
```text
   session_id   |   user_id   |       started_at       |      last_seen_at      |        ended_at        | event_count | is_active  
----------------+-------------+------------------------+------------------------+------------------------+-------------+-----------  
 sess_m4_live_1 | usr_m4_live | 2026-10-10 12:00:00+00 | 2026-10-10 12:20:00+00 | 2026-10-10 12:15:00+00 |           4 | f 
```
**Confirmed Invariants**:
1. `event_count = 4`: Exactly 4 events recorded despite the duplicate delivery.
2. `is_active = f`: Session was NOT silently reopened by the late event.
3. `ended_at = 2026-10-10 12:15:00+00`: Session close timestamp was preserved intact without corruption.
4. `last_seen_at = 2026-10-10 12:20:00+00`: Late activity accurately captured.

#### Step 7: Analytics Overview Endpoint
`GET /api/v1/analytics/overview?from=2026-10-10T00:00:00Z&to=2026-10-10T23:59:59Z`
```json
{
  "from": "2026-10-10T00:00:00Z",
  "to": "2026-10-10T23:59:59Z",
  "totalEvents": 6,
  "uniqueUsers": 3,
  "activeSessions": 0,
  "eventsByType": {
    "PAGE_VIEW": 2,
    "LOGOUT": 1,
    "PRODUCT_VIEW": 1,
    "LOGIN": 1,
    "PURCHASE": 1
  },
  "eventsBySource": {
    "MOBILE": 1,
    "WEB": 5
  }
}
```

#### Step 8: Time-Series Aggregation with UTC Gap-Filling
`GET /api/v1/analytics/timeseries?interval=MINUTE&from=2026-10-10T12:00:00Z&to=2026-10-10T12:25:00Z`
- Output: 26 continuous UTC minute buckets generated with gap-filled zero counts.

#### Step 9: User Lifetime Analytics
`GET /api/v1/analytics/users/usr_m4_live`
```json
{
  "userId": "usr_m4_live",
  "totalEvents": 4,
  "totalSessions": 1,
  "activeSessions": 0,
  "firstSeenAt": "2026-10-10T12:00:00Z",
  "lastSeenAt": "2026-10-10T12:20:00Z",
  "eventsByType": {
    "PAGE_VIEW": 1,
    "LOGOUT": 1,
    "PRODUCT_VIEW": 1,
    "LOGIN": 1
  }
}
```

#### Step 10: Validation Rejection
`GET /api/v1/analytics/timeseries?interval=YEAR`
- HTTP Status: `400 Bad Request`
- Response: `{"status":400,"error":"Bad Request","message":"Invalid interval: 'YEAR'. Allowed intervals are: MINUTE, HOUR, DAY"}`
