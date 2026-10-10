# PulseStream — Architecture Decision Records (ADR)

This document records significant architectural decisions made during the development
of PulseStream. Each entry follows a standard format designed for clarity and for
explaining reasoning in technical interviews.

---

## ADR-001: Modular Monolith over Microservices

**Date**: 2026-10-07  
**Status**: Accepted

### Problem
Should PulseStream be built as a microservices system or as a single deployable unit?

### Options Considered
1. Full microservices (separate services per boundary)
2. Modular monolith (single deployable, clear internal package boundaries)
3. Unstructured monolith (single service, no enforced boundaries)

### Decision
Modular monolith.

### Why
- We have one team and one developer. Microservices add operational overhead (service discovery, distributed tracing, inter-service auth) that provides no benefit at this stage.
- Clear package boundaries (`api/`, `application/`, `domain/`, `messaging/`, `infrastructure/`) enforce separation of concerns without network hops.
- The architecture can evolve to microservices when there is a concrete trigger: e.g., the Kafka consumer layer needs to scale independently from the API layer.
- Testing is dramatically simpler — one application context, one Docker Compose file.

### Tradeoffs
- A single deployment unit limits independent scaling per component.
- A bug anywhere can take down the entire application (vs isolated services).
- The split trigger must be recognized and acted on before scaling problems appear.

### Interview Answer
"I started with a modular monolith because the bounded contexts are already clear in the package structure — `api`, `application`, `domain`, `messaging`, `infrastructure`. The split to microservices has a concrete trigger: when the Kafka consumer throughput needs to scale independently from the ingestion API. At that point, the package boundary already maps to a service boundary."

---

## ADR-002: Apache Kafka for Event Streaming

**Date**: 2026-10-07  
**Status**: Accepted

### Problem
How should activity events flow from the ingestion API to downstream consumers?

### Options Considered
1. Direct database write (synchronous, no queue)
2. RabbitMQ
3. Apache Kafka
4. AWS SQS / SNS (cloud-specific)

### Decision
Apache Kafka.

### Why
- **Durability**: Events are written to disk and retained for 7 days. If a consumer crashes, it can replay from where it left off.
- **Replayability**: Kafka's consumer offset model lets us replay events from any point — essential for backfilling Elasticsearch after downtime.
- **Throughput**: Kafka handles millions of events/second. Activity tracking workloads are high-throughput by nature.
- **Kafka Streams**: Built-in stream processing without a separate cluster.
- **Industry standard**: Kafka is the standard for event streaming in backend engineering.

### What Kafka Does NOT Magically Solve
- Kafka does not prevent duplicate processing (we handle that with idempotency).
- Kafka does not guarantee exactly-once without explicit transactions (we use at-least-once + idempotent consumer).
- A single-broker development setup has no fault tolerance.

### Tradeoffs
- Kafka adds operational complexity (topic management, consumer group lag monitoring).
- Local development requires running a Kafka broker (mitigated by Docker Compose).
- For very low event volumes, Kafka is over-engineered — but this is a portfolio project demonstrating production patterns.

### Interview Answer
"Kafka provides durable, replayable event streaming. If Elasticsearch goes down for 2 hours, events stay in Kafka. When ES comes back, the consumer resumes from its last committed offset and reprocesses. That's not possible with direct writes."

---

## ADR-003: Kafka Streams for Session Aggregation

**Date**: 2026-10-07  
**Status**: Accepted

### Problem
How should we aggregate user sessions from a stream of events?

### Options Considered
1. Application-level timer (scheduled job polling PostgreSQL)
2. Apache Flink (separate cluster)
3. Kafka Streams (embedded in the application)
4. Spark Streaming (too heavy)

### Decision
Kafka Streams.

### Why
- **Embedded**: No separate cluster needed. Kafka Streams runs in the same JVM as our Spring Boot application.
- **Stateful**: Kafka Streams maintains state stores (RocksDB) locally, with changelog topics for fault tolerance.
- **Session Windows**: Kafka Streams has a native `SessionWindows` API with a configurable inactivity gap — exactly what we need.
- **No extra infrastructure**: We already have Kafka. Kafka Streams adds zero infrastructure cost.

### What a Session Is (in PulseStream)
A session groups all events from the same `sessionId`. A session is considered closed when:
1. A `LOGOUT` event is received for that session, OR
2. No events are received for 30 minutes (configurable via `pulsestream.session.timeout-minutes`).

### Tradeoffs
- Kafka Streams state stores are local to the application instance. Scaling to multiple instances requires partition reassignment and state migration.
- The inactivity gap in `SessionWindows` is approximate — Kafka Streams processes events in order but wall-clock time is approximate.

---

## ADR-004: PostgreSQL + Elasticsearch (Split Storage)

**Date**: 2026-10-07  
**Status**: Accepted

### Problem
Where should activity events be stored?

### Options Considered
1. PostgreSQL only
2. Elasticsearch only
3. PostgreSQL for transactional data + Elasticsearch for analytics

### Decision
Split: PostgreSQL for transactional data, Elasticsearch for analytics queries.

### Why — PostgreSQL
- ACID guarantees — each event insert is transactional.
- UNIQUE constraint on `event_id` is our idempotency guard.
- Relational integrity for sessions and users.
- Best tool for: "did this event arrive?", "what is the state of this session?"

### Why — Elasticsearch
- Aggregation queries at scale (top users, event type breakdown, time-range histograms) are far faster in ES than PostgreSQL.
- Full-text search across metadata fields.
- Built-in time-series analysis (date histograms in Kibana).
- Horizontal scaling for read-heavy analytics workloads.

### The Split in Practice
- Kafka consumer writes to PostgreSQL first (durable record).
- Then writes to Elasticsearch (analytics copy).
- If ES write fails, the event is still in PostgreSQL. Reprocessing can rebuild the ES index.
- ES is treated as a derived, eventually-consistent read model — not the source of truth.

### Tradeoffs
- Two writes per event means two failure points.
- ES and PostgreSQL can temporarily diverge if ES is slow.
- Engineers must understand which system to query for which use case.

### Interview Answer
"PostgreSQL is the source of truth. It has the UNIQUE constraint that enforces idempotency. Elasticsearch is a derived read model — built for aggregation queries that would be expensive in PostgreSQL. If ES goes down, we can rebuild it from Kafka. If PostgreSQL goes down, we have a real problem."

---

## ADR-005: Partition Key = userId

**Date**: 2026-10-07  
**Status**: Accepted

### Problem
What should the Kafka partition key be for `user.activity.events`?

### Options Considered
1. No key (round-robin distribution)
2. `eventId` (random distribution)
3. `userId` (per-user ordering)
4. `sessionId` (per-session ordering)

### Decision
`userId`.

### Why
- Events from the same user land in the same partition → **ordering per user is guaranteed**.
- Kafka Streams session aggregation requires all events for a user to be in the same partition for correct windowing.
- `sessionId` would also work but is more granular — multiple sessions per user would be split across partitions, complicating session aggregation.

### The Hot Partition Problem
If one user generates extreme event volume, their partition gets hot (disproportionate load). Mitigation options:
1. Accept it — for an analytics platform, hot users are rare edge cases.
2. Use `userId + bucket` (e.g., hash % 3) for known high-volume users.
3. Split into sub-partitions at the cost of ordering guarantees.

For PulseStream, we accept the tradeoff and document it.

---

## ADR-006: Idempotency Strategy

**Date**: 2026-10-07  
**Status**: Accepted

### Problem
Kafka provides at-least-once delivery. How do we prevent duplicate event processing?

### Decision
Two-layer defense:
1. **Redis TTL key** (fast path): `idempotency:{eventId}` with 24-hour TTL.
2. **PostgreSQL UNIQUE constraint** on `event_id` (durable path): catches duplicates if Redis is down.

### Why Two Layers?
- Redis is O(1) and avoids a database round-trip for duplicate checks.
- Redis can fail or be restarted (keys lost). PostgreSQL is always the fallback.
- The combination provides correctness (PostgreSQL) with performance (Redis).

### What Happens on Retry?
1. Consumer receives event.
2. Check Redis: `exists idempotency:{eventId}` → if true, ACK and skip.
3. Attempt PostgreSQL INSERT → if `DataIntegrityViolationException` (duplicate), ACK and skip.
4. On success: set Redis key with 24h TTL, insert to ES.

### Honest Limitation
This is **at-least-once delivery with idempotent consumers**. It is NOT Kafka's "exactly-once" semantic, which requires:
- Kafka transactional producers
- Transactional consumers (read-process-write transactions)
- Significantly more complexity

For an analytics platform where occasional duplicates are caught by idempotency, this is the correct engineering tradeoff.

---

## ADR-007: Maven over Gradle

**Date**: 2026-10-07  
**Status**: Accepted

### Decision
Apache Maven.

### Why
- Maven's lifecycle phases (validate → compile → test → package → install → deploy) are explicit and well-understood.
- Every Java engineer can read `pom.xml`. Gradle's Kotlin DSL requires additional familiarity.
- Spring Boot's POM parent provides sensible dependency management without custom configuration.
- For a portfolio project prioritizing clarity over build speed, Maven is the correct choice.

### Tradeoff
Gradle has faster incremental builds and a more expressive DSL. If build time becomes a bottleneck in a larger codebase, Gradle is worth considering.

---

## ADR-008: No Authentication in Milestone 1

**Date**: 2026-10-07  
**Status**: Accepted

### Decision
Authentication/authorization is not implemented in M1. The application is designed for easy addition.

### Why
- Authentication adds significant complexity (token validation, filter chains, role management).
- For a local development environment used to demonstrate event streaming and analytics, auth adds noise without value.
- The package structure (`config/`, `api/`) provides clear integration points for Spring Security.

### How to Add Later
1. Add `spring-boot-starter-security` dependency.
2. Create `SecurityConfig.java` in `com.pulsestream.config`.
3. Add JWT filter and role-based access to `/api/v1/events` (write) and `/api/v1/analytics` (read).
4. No other code needs to change — the service and infrastructure layers are already decoupled from HTTP concerns.

### What Is NOT Acceptable
- Hard-coded API keys in source code.
- Secrets in `application.yml` committed to Git.
- All configuration comes from environment variables.

---

## ADR-009: Synchronous Broker Acknowledgment for Event Ingestion (acks=all)

**Date**: 2026-10-09  
**Status**: Accepted

### Problem
When clients submit activity events to `POST /api/v1/events`, should the API return `202 Accepted` immediately (fire-and-forget), or block until Kafka confirms persistence?

### Options Considered
1. **Asynchronous fire-and-forget**: Return `202 Accepted` immediately upon receiving the HTTP payload; publish in background thread.
2. **Synchronous broker acknowledgment**: Wait for Kafka leader + ISR acknowledgment before returning `202 Accepted`.
3. **Transactional Outbox in PostgreSQL**: Write event to PostgreSQL first within a DB transaction, then asynchronously relay to Kafka via CDC (Debezium) or poller.

### Decision
Synchronous broker acknowledgment with `acks=all`, producer idempotence (`enable.idempotence=true`), and a bounded timeout (5000ms).

### Why
- **Durability Guarantee**: Returning `202 Accepted` guarantees the client that the event is durably committed to the Kafka cluster across all in-sync replicas (`acks=all`).
- **No Data Loss on Crash**: If the application server crashes immediately after returning `202`, the event is already committed to Kafka and will not be lost.
- **Backpressure & Fast Failure**: If Kafka becomes unreachable or saturated, client requests fail fast with `503 Service Unavailable` after the configurable timeout rather than filling unbounded JVM in-memory queues and causing OutOfMemory errors.
- **Simplicity**: Avoids the complexity and write-amplification of a full Transactional Outbox pattern while maintaining strict delivery confirmation.

### Tradeoffs
- Higher ingestion latency per request (bounded by Kafka network RTT + disk commit, typically 5-15ms locally).
- Ingestion availability is directly coupled to Kafka availability. If Kafka ISR is degraded or down, ingestion requests are rejected with `503`.

### Interview Answer
"In PulseStream Milestone 2, we return HTTP 202 Accepted only after Kafka acknowledges receipt with `acks=all`. Fire-and-forget APIs can return 202 and subsequently drop messages if the broker is unavailable. Synchronous waiting with a bounded 5-second timeout gives the client guaranteed delivery confirmation while preventing unbounded memory consumption during broker degradation."

---

## ADR-010: Request Correlation Tracing and Standardized Error Contract

**Date**: 2026-10-09  
**Status**: Accepted

### Problem
How should request correlation and error responses be standardized across API and messaging layers?

### Decision
- **MDC Correlation**: An `X-Request-Id` header is captured or generated for every incoming request via `RequestIdFilter`, stored in SLF4J MDC, included in all log outputs, and returned in the HTTP response headers.
- **RFC-7807-Inspired Error Model**: A centralized `GlobalExceptionHandler` converts all validation errors, malformed payloads, invalid enum values, and broker failures into a consistent `ErrorResponse` containing `timestamp`, `status`, `error`, `message`, `path`, and `requestId`.
- **Zero Stack Trace Leaks**: Internal exception stack traces and sensitive payload metadata are logged to server logs only and never exposed to API callers.

---

## ADR-011: Kafka Consumer Persistence, Record Acknowledgment, and Dead-Letter Queue (M3)

**Date**: 2026-10-10  
**Status**: Accepted

### Problem
How should activity events consumed from `user.activity.events` be persisted into PostgreSQL reliably, preventing data loss, infinite consumer loops, duplicate records, and out-of-order commits?

### Options Considered
1. **Auto-commit offsets (`enable.auto.commit=true`)**: Simple, but introduces data loss if the consumer crashes after committing offsets but before persisting to PostgreSQL.
2. **Batch commits**: Commits offsets after a batch of records. Increases throughput, but complicates duplicate tracking and partial failure rollback.
3. **Record-level acknowledgment (`AckMode.RECORD`) with transactional persistence and DLQ**: Commits the Kafka offset strictly after the individual record's database transaction commits successfully.

### Decision
Record-level acknowledgment (`AckMode.RECORD`) combined with an idempotent `@Transactional` persistence service, `ErrorHandlingDeserializer`, bounded backoff retries, and dead-letter routing to `user.activity.dlq`.

### Key Mechanics
1. **Offset Commit After DB Commit**: The listener container only commits the offset of a record after `ActivityEventPersistenceService.persist(event)` successfully commits to PostgreSQL.
2. **Idempotency Guard**:
   - Application-level check: `activityEventRepository.existsByEventId(event.eventId())`. If true, the event is logged and skipped.
   - Database-level fallback: The unique constraint `uq_activity_events_event_id` catches concurrent race conditions via `DataIntegrityViolationException`, which is caught and handled idempotently without throwing.
3. **Safe Deserialization & Poison Pill Isolation**: `ErrorHandlingDeserializer` catches malformed payloads and passes the error to `DefaultErrorHandler`. Deserialization errors are marked non-retryable and routed directly to `user.activity.dlq` using `DeadLetterPublishingRecoverer`, preventing toxic poison pills from halting partition consumption.
4. **Transient Database Outage Handling**: Database connection failures trigger bounded retries (configured via `FixedBackOff` with 2 retries). If retries exhaust, the record is published to DLQ and the offset is advanced, preventing partition blockage while preserving diagnostic context.

### Delivery Semantic
At-least-once delivery with idempotent persistence. Not Kafka transactional exactly-once, but guarantees no message loss and no duplicate database rows.

---

## ADR-012: Real-Time Analytics and Session Aggregation (M4)

**Date**: 2026-10-10  
**Status**: Accepted

### Problem
How to provide real-time platform activity metrics, aggregated time-series trends, per-user summaries, and active session tracking using the existing PostgreSQL and Kafka infrastructure without introducing Redis, Elasticsearch, or additional containers?

### Key Architectural Decisions

1. **Transaction-Safe Atomic Event & Session Aggregation**:
   - In Spring Data JPA, catching `DataIntegrityViolationException` inside an active `@Transactional` boundary causes Hibernate to flag the physical transaction as rollback-only.
   - To avoid transaction poisoning while preserving the database unique constraint (`uq_activity_events_event_id`), event insertion executes native parameterized SQL via `JdbcTemplate`:
     ```sql
     INSERT INTO activity_events (...) VALUES (...) ON CONFLICT (event_id) DO NOTHING
     ```
   - Session aggregation runs strictly when `affectedRows == 1` (a genuinely new event was inserted). Duplicate deliveries resolve safely with `affectedRows == 0`, skipping session re-aggregation and preserving exact `event_count` metrics.
   - Event insertion and session aggregation participate in the identical database transaction, ensuring atomicity.

2. **Closed-Session Invariants & Lifecycle Integrity**:
   - A session closed by an explicit `LOGOUT` event or inactivity expiration sweep must **never** be silently reopened by late-arriving events.
   - For closed sessions (`isActive == false`), late events increment `event_count` and adjust `last_seen_at` without mutating `isActive` or corrupting `endedAt`.
   - Out-of-order events adjust `started_at` backwards if earlier than the current session start time.

3. **Inactivity Expiration Sweep**:
   - An asynchronous `@Scheduled` periodic sweep executes every 60 seconds (`pulsestream.session.expiration-check-interval-ms`).
   - Executes an atomic bulk update:
     ```sql
     UPDATE user_sessions u SET u.isActive = false, u.endedAt = u.lastSeenAt
     WHERE u.isActive = true AND u.lastSeenAt <= :cutoff
     ```
   - Partial index `idx_us_active_last_seen ON user_sessions(last_seen_at DESC) WHERE is_active = TRUE` ensures this query remains an efficient index scan regardless of table size.

4. **Bounded Time-Series Analytics & Continuous UTC Gap-Filling**:
   - Intervals are strictly validated against an allowlist: `MINUTE` (max range: 6h), `HOUR` (max range: 14d), `DAY` (max range: 90d).
   - Hard ceiling of 500 buckets prevents memory exhaustion and unbounded database load.
   - Static parameterized native queries avoid any dynamic SQL string concatenation.
   - Continuous UTC gap-filling generates missing buckets populated with 0 counts for reliable dashboard charting.

---

## ADR-013: Caching and Search Infrastructure Evaluation: Retaining PostgreSQL-Only Architecture (M6)

**Date**: 2026-10-10  
**Status**: Accepted

### Problem
Should PulseStream introduce Redis for caching analytics queries and/or Elasticsearch for activity event exploration, or does the optimized PostgreSQL architecture sufficiently satisfy performance and scale requirements?

### Options Considered
1. **Redis Caching**: Cache `/api/v1/analytics/overview` and `/timeseries` results with TTL (e.g. 15s to 60s) or event-driven cache eviction.
2. **Elasticsearch Indexing**: Stream events to Elasticsearch for multi-field filtering and aggregations alongside PostgreSQL.
3. **PostgreSQL-Only Analytics**: Retain PostgreSQL as the single source of truth for both persistence and analytics, leveraging specialized indexes (V4 + V5).

### Decision
**Retain PostgreSQL-only architecture**. Intentionally do NOT adopt Redis caching or Elasticsearch at this milestone.

### Rationale and Measured Evidence

1. **Measured PostgreSQL Query Latency (Sub-2ms)**:
   - EXPLAIN ANALYZE on representative datasets demonstrated that composite indexes (`idx_ae_ts_user`, `idx_ae_user_ts`, `idx_ae_ts_source`, `idx_us_active_last_seen`) enable Index-Only Scans with 0 heap fetches.
   - Measured query execution times:
     - Total events & active users: **0.17 ms**
     - Event type breakdown: **0.16 ms**
     - Source breakdown: **0.15 ms**
     - Active session count: **0.14 ms**
     - 24-bucket time-series aggregation: **1.68 ms**
   - At sub-2ms query times, network round-trips to an external Redis cache (~0.5ms–1.5ms) would provide negligible performance benefit while significantly complicating the architecture.

2. **The Streaming Cache Invalidation Hazard**:
   - In a real-time event streaming pipeline, activity events arrive continuously at high frequency.
   - If analytics results are cached, either:
     - Every incoming event invalidates the cache, driving the cache hit ratio near zero and causing cache-stampede latency spikes on the database, OR
     - The cache serves stale data under a fixed TTL, violating real-time platform requirements.
   - Direct execution of indexed SQL queries against PostgreSQL guarantees strictly fresh, consistent analytics without cache-coherency bugs.

3. **Elasticsearch Dual-Write and Operational Overhead**:
   - Elasticsearch introduces dual-write hazards, eventual consistency divergence, index mappings lifecycle maintenance, and extra container memory footprint (~1GB+ heap).
   - The platform's existing analytics endpoints (`overview`, `timeseries`, `users/{userId}`, `sessions/active`) do not require full-text fuzzy search or ad-hoc multi-dimensional faceted navigation.
   - PostgreSQL provides atomic ACID transactions, identical time-series capabilities, and single-source-of-truth reliability.

4. **Failure Surface Reduction**:
   - Omitting unnecessary external systems eliminates distributed failure modes (e.g. Redis out-of-memory, Elasticsearch cluster yellow/red state, split-brain recovery).
   - Actuator health reporting reflects true application health without false-negative alarms from unused external services.

### Future Triggers for Reconsideration
- **Redis**: Re-evaluate if read query traffic exceeds 10,000 req/sec on identical fixed time windows (e.g., public dashboard with 1-minute caching).
- **Elasticsearch**: Re-evaluate if business requirements introduce unstructured free-text search across arbitrary JSON metadata or log exploration.

### Interview Answer
"In Milestone 6, we rigorously evaluated Redis and Elasticsearch against real query execution plans rather than blindly adding tools to the stack. With our V5 composite indexes, PostgreSQL delivers analytics responses in under 2 milliseconds via index-only scans. Under continuous streaming ingestion, caching creates an invalidation thrashing dilemma with zero freshness guarantees, while Elasticsearch introduces dual-write complexity without any text-search requirement. We made the disciplined architectural decision to retain a PostgreSQL-only data store, keeping the system resilient, transactional, and simple."



