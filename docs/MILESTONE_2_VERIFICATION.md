# PulseStream: Milestone 2 Manual Verification & Environment Check

## 1. Environment Versions and Checks

| Component | Expected | Observed Result | Status |
|---|---|---|---|
| **Java Version** | Java 21 | OpenJDK `21.0.8` (Temurin-21.0.8+9, 64-Bit Server VM) | **PASS** |
| **Maven Wrapper** | Available | Apache Maven `3.9.16` (`.\mvnw.cmd -version`) | **PASS** |
| **Docker CLI** | Available | Docker CLI `29.8.1`, build `4a63305` | **PASS** |
| **Docker Context** | `desktop-linux` | `desktop-linux *` (`npipe:////./pipe/dockerDesktopLinuxEngine`) | **PASS** |
| **Docker Engine** | Running | Running (Server Version: `29.8.1`, WSL2 Kernel: `6.6.87.2`) | **PASS** |
| **Port 9092 (Kafka)** | In use by `pulsestream-kafka` | `0.0.0.0:9092->9092/tcp` healthy | **PASS** |
| **Port 8090 (Kafka UI)** | In use by `pulsestream-kafka-ui` | `0.0.0.0:8090->8080/tcp` active | **PASS** |
| **Port 5433 (PostgreSQL)** | In use by `pulsestream-postgres` | `0.0.0.0:5433->5432/tcp` healthy (re-mapped from 5432) | **PASS** |
| **Port 8081 (Application)** | In use by PulseStream app | Tomcat HTTP listening on 8081 (re-mapped from 8080) | **PASS** |
| **Git Status** | Inspected | Not a git repository (`fatal: not a git repository`) | **PASS** |

---

## 2. Infrastructure Container Verification

PulseStream infrastructure containers running via Docker Compose with port conflict offsets:

| Container Name | Image | Port Mapping | Health Status | Verification Status |
|---|---|---|---|---|
| `pulsestream-postgres` | `postgres:16-alpine` | `5433:5432` | `healthy` (`pg_isready` check passed) | **PASS** |
| `pulsestream-kafka` | `apache/kafka:3.7.0` | `9092:9092` | `healthy` (broker API check passed) | **PASS** |
| `pulsestream-kafka-ui` | `provectuslabs/kafka-ui:latest` | `8090:8080` | `Up` (HTTP 200 on `http://localhost:8090`) | **PASS** |

---

## 3. Dependency & Flyway Diagnostics

- **Spring Boot Version**: `4.0.0` (Spring Framework `7.0.1`)
- **Flyway Dependencies**:
  - `org.springframework.boot:spring-boot-starter-flyway:4.0.0`
  - `org.flywaydb:flyway-core:11.14.1`
  - `org.flywaydb:flyway-database-postgresql:11.14.1`
- **Flyway Migrations Applied to PostgreSQL (`pulsestream` database on port 5433)**:
  - `V1__create_users_table.sql` — `SUCCESS`
  - `V2__create_activity_events_table.sql` — `SUCCESS`
  - `V3__create_user_sessions_table.sql` — `SUCCESS`
  - Schema history table `flyway_schema_history` populated with 3 successful records.
  - Tables created and verified: `users`, `activity_events`, `user_sessions`, `flyway_schema_history`.
- **Status**: **PASS**

---

## 4. Automated Tests (`.\mvnw.cmd test`)

Command executed: `.\mvnw.cmd test`
- **Test execution summary**:
  - `com.pulsestream.api.controller.EventControllerTest` passed
  - `com.pulsestream.PulseStreamApplicationTests` passed
- **Results**: `Tests run: 15, Failures: 0, Errors: 0, Skipped: 0`
- **Build Status**: `BUILD SUCCESS` (Total time: 44.089 s)
- **Status**: **PASS**

---

## 5. Live Application Verification

Application launched on `http://localhost:8081` with local configuration:
- Database: `jdbc:postgresql://localhost:5433/pulsestream`
- Kafka Broker: `localhost:9092`
- Server Port: `8081`

### A. Health & Actuator Endpoints (`http://localhost:8081/actuator/health`)
- **Liveness (`/actuator/health/liveness`)**: `{"status":"UP"}` — **PASS**
- **Readiness (`/actuator/health/readiness`)**: `{"status":"UP"}` — **PASS**
- **Database Component (`db`)**: `{"status":"UP","details":{"database":"PostgreSQL","validationQuery":"isValid()"}}` — **PASS**
- **DiskSpace Component (`diskSpace`)**: `{"status":"UP"}` — **PASS**
- *Note*: `redis` component reports `DOWN` on full aggregate `/actuator/health` as Redis is scheduled for Milestone 3+ and is not currently started.

### B. OpenAPI & Swagger UI
- **OpenAPI 3.1 Spec Endpoint (`/v3/api-docs`)**: HTTP `200 OK`, returning full schema for `EventRequest`, `EventResponse`, and `ErrorResponse` — **PASS**
- **Swagger UI (`/swagger-ui.html`)**: HTTP `302 Found` redirecting to `/swagger-ui/index.html` (HTTP `200 OK`) — **PASS**

### C. Event Ingestion API (`POST /api/v1/events`)
- **Request**:
  ```http
  POST /api/v1/events HTTP/1.1
  Host: localhost:8081
  Content-Type: application/json

  {
    "userId": "usr_verify_m2_1001",
    "sessionId": "sess_verify_m2_2002",
    "eventType": "PAGE_VIEW",
    "timestamp": "2026-10-10T11:53:00Z",
    "source": "WEB",
    "metadata": {
      "pageUrl": "/milestone-2/verification",
      "referrer": "https://pulsestream.test"
    }
  }
  ```
- **Observed Response**:
  - **HTTP Status**: `202 Accepted`
  - **Header**: `X-Request-Id: 4c42b5a2-ffca-462f-89f7-37b45f624af4`
  - **Body**:
    ```json
    {
      "eventId": "3f73b855-16be-4839-9f97-2c81c21c4033",
      "status": "ACCEPTED",
      "message": "Event accepted and published to stream",
      "timestamp": "2026-10-10T06:23:20.208708900Z"
    }
    ```
  - **Status**: **PASS**

### D. Kafka Topic Verification (`user.activity.events`)
- Topic pre-configured with 6 partitions: `user.activity.events`.
- Partition offset query via `kafka-get-offsets.sh` verified message published to partition 5:
  `user.activity.events:5:1`
- Message consumption verified via `kafka-console-consumer.sh`:
  - **Partition**: `5`
  - **Key**: `usr_verify_m2_1001` (matches `userId` partitioning requirement)
  - **Value**:
    ```json
    {
      "eventId": "3f73b855-16be-4839-9f97-2c81c21c4033",
      "userId": "usr_verify_m2_1001",
      "sessionId": "sess_verify_m2_2002",
      "eventType": "PAGE_VIEW",
      "timestamp": "2026-10-10T11:53:00Z",
      "source": "WEB",
      "metadata": {
        "pageUrl": "/milestone-2/verification",
        "referrer": "https://pulsestream.test"
      }
    }
    ```
- **Status**: **PASS**

### E. Kafka UI Verification (`http://localhost:8090`)
- Web UI accessible at `http://localhost:8090` returning `HTTP 200 OK`.
- Cluster `pulsestream-local` healthy and reporting topic metrics.
- **Status**: **PASS**

---

## 6. Milestone 2 Verification Summary

| Check | Target | Result | Status |
|---|---|---|---|
| M2-01 | Environment & Port Free/Mapped Verification | No port conflicts (Postgres on 5433, App on 8081) | **PASS** |
| M2-02 | Infrastructure Health | Postgres, Kafka, Kafka UI all healthy | **PASS** |
| M2-03 | Database Migrations | Flyway applied V1, V2, V3 successfully | **PASS** |
| M2-04 | Unit & Context Tests | `.\mvnw.cmd test` (15/15 passed) | **PASS** |
| M2-05 | Application Startup | Spring Boot 4.0.0 started on port 8081 | **PASS** |
| M2-06 | Event Ingestion API | `POST /api/v1/events` returned `202 Accepted` | **PASS** |
| M2-07 | Kafka Message Publication | Message in `user.activity.events` with `userId` key | **PASS** |
| M2-08 | OpenAPI / Swagger UI | `/v3/api-docs` and `/swagger-ui.html` HTTP 200 | **PASS** |
| M2-09 | Health Endpoints | Liveness, readiness, and DB health UP | **PASS** |
| M2-10 | Milestone 3 Scope Separation | No Milestone 3 code or features initiated | **PASS** |
