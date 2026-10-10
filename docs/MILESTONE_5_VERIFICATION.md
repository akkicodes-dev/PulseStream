# PulseStream: Milestone 5 Verification Report

## 1. Overview & Scope

Milestone 5 focuses on advanced analytics correctness, query plan evaluation, index optimization, and deterministic database validation:
1. **Query Plan & EXPLAIN ANALYZE Audit**:
   - Benchmarked time-series queries and user history queries against realistic 5,000-event datasets in PostgreSQL 16.
   - Identified heap fetch overhead on time-series queries aggregating `COUNT(DISTINCT user_id)`.
   - Introduced `V5__optimize_analytics_indexes.sql`:
     - `idx_ae_ts_user ON activity_events(event_timestamp DESC, user_id)`: Transforms hourly and minute time-series queries from heap-dependent index scans to **Index-Only Scans with 0 heap fetches** (query tuple width dropped from 524 bytes to 15 bytes).
     - `idx_ae_user_ts ON activity_events(user_id, event_timestamp DESC)`: Optimizes user lifetime event queries and paginated user activity history.
2. **UTC Query Truncation & Timezone Semantics**:
   - Diagnosed JDBC timezone pitfall: PostgreSQL `date_trunc(..., ts AT TIME ZONE 'UTC')` yields `timestamp without time zone`, causing JDBC clients in non-UTC JVMs to interpret timestamps in local time and skew buckets.
   - Fixed static repository queries to return `(date_trunc(...) AT TIME ZONE 'UTC') AS bucket`, guaranteeing exact UTC `TIMESTAMPTZ` mapping across all JVM environments.
3. **Database-Backed Integration Testing (`AnalyticsQueryIntegrationTest`)**:
   - Deterministic test suite against PostgreSQL validating exact seeded totals, distinct user counts, event type and source distributions, session tracking, continuous gap-filling, and user lifetime metrics.
4. **Boundary & Edge-Case Unit Coverage (`AnalyticsServiceTest`)**:
   - Added tests for empty datasets, single-point boundaries (`from == to`), missing time buckets, and users without prior activity.

---

## 2. PostgreSQL EXPLAIN ANALYZE Benchmarks

### A. Hourly Time-Series Aggregation Query
```sql
EXPLAIN ANALYZE 
SELECT date_trunc('hour', event_timestamp AT TIME ZONE 'UTC') AS bucket, 
       COUNT(*) AS total_events, 
       COUNT(DISTINCT user_id) AS unique_users 
FROM activity_events 
WHERE event_timestamp >= NOW() - interval '24 hours' AND event_timestamp <= NOW() 
GROUP BY bucket ORDER BY bucket ASC;
```

#### Before Optimization (Without `idx_ae_ts_user`):
```text
GroupAggregate (cost=8.31..8.34 rows=1 width=24) (actual time=1.457..1.659 rows=25 loops=1)
  -> Sort (cost=8.31..8.32 rows=1 width=524) (actual time=1.409..1.466 rows=1439 loops=1)
       Sort Key: (date_trunc('hour'::text, (event_timestamp AT TIME ZONE 'UTC'::text))), user_id
       Sort Method: quicksort Memory: 105kB
       -> Index Scan using idx_ae_ts_source on activity_events (cost=0.28..8.30 rows=1 width=524)
            Index Cond: ((event_timestamp >= (now() - '24:00:00'::interval)) AND (event_timestamp <= now()))
Planning Time: 1.402 ms
Execution Time: 1.832 ms
```
*Observation*: Engine falls back to `idx_ae_ts_source` and must fetch all 1,439 rows from the table heap to retrieve `user_id` (tuple width = 524 bytes).

#### After Optimization (With `idx_ae_ts_user`):
```text
GroupAggregate (cost=136.62..171.67 rows=1402 width=24) (actual time=1.360..1.547 rows=25 loops=1)
  -> Sort (cost=136.62..140.12 rows=1402 width=15) (actual time=1.350..1.404 rows=1439 loops=1)
       Sort Key: (date_trunc('hour'::text, (event_timestamp AT TIME ZONE 'UTC'::text))), user_id
       Sort Method: quicksort Memory: 105kB
       -> Index Only Scan using idx_ae_ts_user on activity_events (cost=0.29..63.34 rows=1402 width=15)
            Index Cond: ((event_timestamp >= (now() - '24:00:00'::interval)) AND (event_timestamp <= now()))
            Heap Fetches: 0
Planning Time: 1.277 ms
Execution Time: 1.680 ms
```
*Result*: **Index Only Scan achieved with 0 heap fetches** and tuple width reduced by 97% (from 524 bytes down to 15 bytes).

---

## 3. Automated Test Results (`.\mvnw.cmd test`)

All **52 automated tests** passed with 0 failures and 0 errors:

```text
[INFO] Results:
[INFO] 
[INFO] Tests run: 52, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  37.225 s
```

### Test Breakdown by Component
| Test Class | Category | Scenarios Covered | Status |
|---|---|---|---|
| `AnalyticsQueryIntegrationTest` | Integration (PostgreSQL) | Exact seeded metrics, distinct user counts, type & source breakdowns, continuous gap-filling, user lifetime history | **PASS** (3/3) |
| `AnalyticsServiceTest` | Unit (Mockito) | Overview calculations, empty dataset handling, `from == to` boundary, interval allowlist, max duration limits, gap-filling, empty user handling | **PASS** (9/9) |
| `AnalyticsControllerTest` | Unit (MockMvc) | REST contracts, RFC-7807 error responses, PageResponse pagination, input validation | **PASS** (5/5) |
| `UserSessionAggregationServiceTest` | Unit (Mockito) | Session lifecycle, closed-session non-reopening invariant, late-event safety, expiration sweep | **PASS** (7/7) |
| `ActivityEventPersistenceServiceTest` | Unit (Mockito) | Transaction-safe ON CONFLICT, duplicate detection without session re-aggregation, atomic rollback | **PASS** (6/6) |
| Existing M1–M3 Test Suites | Unit & Integration | Ingestion, producer, consumer, DLQ recovery, smoke tests | **PASS** (22/22) |

---

## 4. Verification Checkpoint

- **Schema Migration**: `V5__optimize_analytics_indexes.sql` verified and applied in PostgreSQL 16.
- **Index Plans**: Confirmed `Index Only Scan` (`Heap Fetches: 0`) for time-series aggregation.
- **Regression**: 100% backward compatible with M1–M4.
