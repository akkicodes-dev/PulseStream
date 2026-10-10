# PulseStream
### Real-Time User Activity & Analytics Platform

> **Portfolio Project** | Java 21 · Spring Boot 4 · Apache Kafka · Kafka Streams · Elasticsearch · PostgreSQL · Redis · Docker

---

## Overview

PulseStream is a production-style backend platform that ingests user activity events in real-time, streams them through Apache Kafka, aggregates user sessions via Kafka Streams, stores analytics data in Elasticsearch, and exposes a clean REST API for querying activity and analytics.

Built to demonstrate backend engineering depth: event-driven architecture, stream processing, idempotency, observability, and clean API design — all in a codebase that is understandable, tested, and interview-defensible.

---

## Problem Statement

Modern applications generate continuous streams of user activity: clicks, page views, purchases, searches. Capturing this data reliably, processing it at scale, and making it queryable in real-time requires an architecture that most REST-only CRUD backends cannot handle.

PulseStream solves:
- **Reliable ingestion** — validated, idempotent event capture
- **Async processing** — decoupled via Kafka (API is never blocked by analytics processing)
- **Session intelligence** — Kafka Streams aggregates events into user sessions with a configurable inactivity timeout
- **Analytics at scale** — Elasticsearch powers fast aggregation queries
- **Operational visibility** — Kibana dashboards + Spring Boot Actuator metrics

---

## Features (Planned by Milestone)

| Feature | Milestone | Status |
|---------|-----------|--------|
| Project foundation, Docker Compose | M1 | ✅ Complete |
| Event ingestion API (`POST /api/v1/events`) | M2 | ✅ Complete |
| Kafka producer + consumer + idempotency | M3 | ✅ Complete |
| Real-time analytics + session aggregation | M4 | ✅ Complete |
| Elasticsearch analytics + query API | M5 | 🔲 Planned |
| Observability (structured logging, metrics) | M6 | 🔲 Planned |
| Full test suite + documentation | M7 | 🔲 Planned |
| Load testing with JMeter + results | M8 | 🔲 Planned |

---

## Architecture

```
Client
  │  HTTP REST
  ▼
Spring Boot Application (Modular Monolith)
  │
  ├── Event Ingestion API  →  Validation  →  Kafka Producer
  │                                               │
  │                                    ┌──────────┤
  │                                    │          │
  │                                    ▼          ▼
  │                             Kafka Streams  Activity Consumer
  │                             (Session Agg)  (Persist + Index)
  │                                    │          │
  └── Analytics Query API  ◄───────────┴──────────┘
                                       │
                              ┌────────┴────────┐
                              ▼                 ▼
                          PostgreSQL       Elasticsearch
                        (Transactional)    (Analytics)
                                               │
                                           Kibana
```

See [docs/ARCHITECTURE_DECISIONS.md](docs/ARCHITECTURE_DECISIONS.md) for detailed reasoning behind every major design choice.

---

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Language | Java | 21 (LTS) |
| Framework | Spring Boot | 4.0 |
| Build | Maven | 3.9+ |
| Messaging | Apache Kafka (KRaft mode) | 3.7 |
| Stream Processing | Kafka Streams | 3.7 |
| Relational DB | PostgreSQL | 16 |
| Search / Analytics | Elasticsearch | 8.13 |
| Visualization | Kibana | 8.13 |
| Cache | Redis | 7 |
| Containers | Docker + Docker Compose | v2 |
| API Docs | SpringDoc OpenAPI | 2.x |
| Testing | JUnit 5 + Mockito + Testcontainers | — |
| Observability | Spring Boot Actuator + Micrometer | — |

---

## Local Setup

### Prerequisites

- Docker Desktop (with Docker Compose v2)
- Java 21 (Temurin recommended)
- Git

Maven is **not required globally** — the project ships with `mvnw` (Maven Wrapper).

### 1. Clone the repository

```bash
git clone https://github.com/your-username/pulsestream.git
cd pulsestream
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env if you need to change default ports or credentials
```

### 3. Start infrastructure

```bash
docker compose up -d
```

Wait for all services to be healthy:

```bash
docker compose ps
```

Expected output (all services `healthy`):
```
NAME                        STATUS
pulsestream-postgres        running (healthy)
pulsestream-kafka           running (healthy)
pulsestream-kafka-ui        running
pulsestream-elasticsearch   running (healthy)
pulsestream-kibana          running (healthy)
pulsestream-redis           running (healthy)
```

### 4. Start the application

```bash
./mvnw spring-boot:run
```

On Windows:
```cmd
mvnw.cmd spring-boot:run
```

### 5. Verify

| Service | URL |
|---------|-----|
| Spring Boot API | http://localhost:8080 |
| Actuator Health | http://localhost:8080/actuator/health |
| Swagger UI | http://localhost:8080/swagger-ui.html |
| Kafka UI | http://localhost:8090 |
| Kibana | http://localhost:5601 |
| Elasticsearch | http://localhost:9200 |

---

## Environment Variables

See [.env.example](.env.example) for the full list with descriptions.

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PASSWORD` | `pulsestream_dev` | **Change in production** |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `ELASTICSEARCH_HOST` | `localhost` | Elasticsearch host |
| `REDIS_HOST` | `localhost` | Redis host |
| `SESSION_TIMEOUT_MINUTES` | `30` | Session inactivity window |

---

## Running Tests

```bash
# Unit tests only (no infrastructure required)
./mvnw test

# Integration tests (requires Docker for Testcontainers)
./mvnw verify
```

---

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|-------|-----------|-----------|---------|
| `user.activity.events` | 6 | 7 days | Raw validated events |
| `user.activity.processed` | 6 | 7 days | Enriched processed events |
| `user.activity.dlq` | 1 | 30 days | Failed / unprocessable events |

**Partition key**: `userId` — ensures per-user event ordering.

---

## Database Schema

Four migrations managed by Flyway:
- `V1__create_users_table.sql`: Lightweight user reference.
- `V2__create_activity_events_table.sql`: Canonical event record (UNIQUE on `event_id`, JSONB metadata, check constraints).
- `V3__create_user_sessions_table.sql`: Aggregated session state (UNIQUE on `session_id`, `event_count`, `last_seen_at`, `ended_at`).
- `V4__add_analytics_indexes.sql`: Compound time-series indexes (`idx_ae_ts_type`, `idx_ae_ts_source`) and active session partial index (`idx_us_active_last_seen`).

---

## API Endpoints

Full interactive OpenAPI / Swagger UI documentation available at `http://localhost:8081/swagger-ui.html`.

| Method | Path | Description | Milestone |
|--------|------|-------------|-----------|
| `POST` | `/api/v1/events` | Ingest user activity event into Kafka pipeline (202 Accepted) | M2 |
| `GET` | `/api/v1/analytics/overview` | Platform activity totals, unique users, active sessions, breakdowns | M4 |
| `GET` | `/api/v1/analytics/timeseries` | Continuous UTC time-series with zero-filled gaps (`MINUTE`, `HOUR`, `DAY`) | M4 |
| `GET` | `/api/v1/analytics/users/{userId}` | Lifetime user activity summary, sessions, and event type breakdown | M4 |
| `GET` | `/api/v1/analytics/sessions/active` | Paginated active sessions within inactivity window | M4 |

---

## Design Decisions

See [docs/ARCHITECTURE_DECISIONS.md](docs/ARCHITECTURE_DECISIONS.md) for detailed ADRs covering:

- Why Kafka? Why not a message queue?
- Why Kafka Streams for session aggregation?
- Why PostgreSQL + Elasticsearch (split storage)?
- Why `userId` as the partition key?
- Why two-layer idempotency (Redis + PostgreSQL)?
- Why Maven over Gradle?
- Why no microservices in M1?

---

## Load Testing

_(Milestone 8 — results will be added after JMeter test runs)_

---

## License

The original source code in this repository is licensed under the **MIT License**.  
See [LICENSE](LICENSE) for the full text and scope clarification.

Third-party dependencies retain their respective licenses.  
No source code was copied from any third-party project.
