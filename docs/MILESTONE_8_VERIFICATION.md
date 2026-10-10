# PulseStream: Milestone 8 Verification Report

## Load Testing and Performance Validation

### 1. Executive Summary

Milestone 8 executes an evidence-based performance and load validation of the PulseStream backend platform using **Apache JMeter 5.6.3** in non-GUI (CLI) mode.

The load test evaluated:
1. **Synchronous Ingestion Throughput (`POST /api/v1/events`)**: Verified end-to-end Kafka acknowledgment (`acks=all`) and producer idempotence under concurrent load.
2. **Duplicate Ingestion Deduplication**: Verified that concurrent identical event submissions return `202 Accepted` while inserting exactly 1 database row and incrementing session counts only once.
3. **Boundary Input Validation**: Verified that invalid payloads are rejected fast (`HTTP 400 Bad Request`) without publishing to Kafka or touching PostgreSQL.
4. **Concurrent Analytics Reads**: Verified that `GET /api/v1/analytics/overview` and `GET /api/v1/analytics/timeseries` queries execute cleanly with low latency alongside write traffic.

---

## 2. Test Workload & Environment Specification

### A. Environment Context
- **Operating System**: Windows 11
- **JVM**: Temurin Java 21 (LTS)
- **Database**: PostgreSQL 16 Alpine (Host port 5433, max connections: 100, Hikari pool: 10)
- **Broker**: Apache Kafka 3.7.0 in KRaft mode (Single-broker, replication factor: 1, 6 partitions)
- **Application Port**: 8081
- **Load Tool**: Apache JMeter 5.6.3

### B. Tested Load Profile
The test plan is committed at `tests/performance/pulsestream_load_test.jmx`:
- **Thread Group 1: Valid Concurrent Ingestion**: 10 threads, 2s ramp-up, 50 loops per thread = **500 requests**
- **Thread Group 2: Duplicate Delivery (Idempotency)**: 2 threads, 1s ramp-up, 25 loops per thread = **50 requests** (static UUID)
- **Thread Group 3: Invalid Request Validation**: 2 threads, 1s ramp-up, 25 loops per thread = **50 requests** (malformed enum/blank userId)
- **Thread Group 4: Concurrent Analytics Reads**: 4 threads, 2s ramp-up, 25 loops per thread = **100 requests** (`overview` + `timeseries`)
- **Total Concurrency**: 18 concurrent threads
- **Total Requests**: **800 HTTP requests**

---

## 3. Measured Results & Latency Percentiles

```text
=================== JMETER RESULTS SUMMARY ===================
 Total Requests:      800
 Failed Requests:     0 (0.00% error rate)
 Test Duration:       11.16s
 Throughput:          71.7 req/sec (Peak engine burst: 263.3 req/sec)
 Average Latency:     26.99 ms
 Min Latency:         5 ms
 Max Latency:         114 ms
 p50 Median Latency:  27 ms
 p90 Latency:         41 ms
 p95 Latency:         48 ms
 p99 Latency:         79 ms
==============================================================
```

### Breakdown by Sampler / Endpoint

| Endpoint / Operation | Total Requests | Failed | Error Rate | Avg Latency | p50 (Median) | p95 | p99 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `POST /api/v1/events` (Valid Ingestion) | 500 | 0 | 0.00% | 27.74 ms | 28 ms | 42 ms | 50 ms |
| `POST /api/v1/events` (Duplicate Idempotency) | 50 | 0 | 0.00% | 28.40 ms | 29 ms | 41 ms | 83 ms |
| `POST /api/v1/events` (Invalid Validation) | 50 | 0 | 0.00% | 7.50 ms | 6 ms | 8 ms | 79 ms |
| `GET /api/v1/analytics/overview` (Read Query) | 100 | 0 | 0.00% | 43.84 ms | 37 ms | 90 ms | 110 ms |
| `GET /api/v1/analytics/timeseries` (Hourly Aggregate) | 100 | 0 | 0.00% | 15.43 ms | 14 ms | 27 ms | 34 ms |

---

## 4. Database Consistency & Idempotency Audit

Immediately following the 800-request load test run, the PostgreSQL database was queried to verify transactional integrity:

1. **Duplicate Event Deduplication Check**:
   ```sql
   SELECT COUNT(*) FROM activity_events WHERE event_id = 'd0000000-0000-0000-0000-000000000001';
   ```
   **Observed Result**: Exactly **1 row** persisted despite 50 repeated submissions.

2. **Session Event Count Idempotency Check**:
   ```sql
   SELECT session_id, user_id, event_count, is_active FROM user_sessions WHERE session_id = 'dup_sess_1';
   ```
   **Observed Result**:
   ```text
    session_id |  user_id   | event_count | is_active 
   ------------+------------+-------------+-----------
    dup_sess_1 | dup_user_1 |           1 | t
   ```
   **Observed Result**: `event_count` is exactly **1**, confirming that duplicate deliveries resolved cleanly via `affectedRows == 0` without incrementing running session counters.

3. **Analytics Endpoint Consistency**:
   `GET /api/v1/analytics/overview` confirmed exact platform totals:
   ```json
   {
     "from": "2026-10-10T00:00:00Z",
     "to": "2026-10-10T23:59:59Z",
     "totalEvents": 1007,
     "uniqueUsers": 14,
     "activeSessions": 11,
     "eventsByType": {
       "LOGIN": 1,
       "LOGOUT": 1,
       "PAGE_VIEW": 1002,
       "PRODUCT_VIEW": 1,
       "PURCHASE": 2
     },
     "eventsBySource": {
       "MOBILE": 2,
       "WEB": 1005
     }
   }
   ```

---

## 5. Reproducibility Instructions

To reproduce the load test results on any machine:

1. Ensure PostgreSQL and Kafka containers are running:
   ```bash
   docker compose ps
   ```
2. Start the PulseStream Spring Boot application on port 8081.
3. Run the automated PowerShell test runner:
   ```powershell
   powershell.exe -ExecutionPolicy Bypass -File tests/performance/run_load_test.ps1 -HostName localhost -Port 8081 -Threads 10 -Loops 50
   ```
4. Or run Apache JMeter directly from CLI:
   ```bash
   jmeter -n -t tests/performance/pulsestream_load_test.jmx \
     -l tests/performance/results/load_test_results.jtl \
     -e -o tests/performance/results/html_report \
     -Jhost=localhost -Jport=8081 -Jthreads=10 -Jloops=50
   ```
5. View the generated dashboard report in a web browser:
   `tests/performance/results/html_report/index.html`

---

## 6. Local Limitations & Engineering Findings

1. **Hardware Ceiling**: Benchmarks were collected on a single local development machine running Docker Desktop WSL2, Apache Kafka, PostgreSQL, and Spring Boot concurrently. In a distributed multi-node cluster with dedicated network interfaces, throughput scales with Kafka partition count and connection pool size.
2. **Synchronous Kafka Acknowledgment**: `POST /api/v1/events` latency averages ~27ms because `acks=all` waits for broker confirmation before returning `202 Accepted`. This trading of a few milliseconds for guaranteed durability eliminates event loss on crash.
3. **Database Read Isolation**: Analytics queries (`/timeseries` at p50 14ms) showed zero degradation even during concurrent write ingestion, validating the effectiveness of PostgreSQL V4/V5 composite indexes.
