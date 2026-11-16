# 01 — Developer Onboarding & Quick-Start Guide

Welcome to **Project Ariel**! Whether you are a **Junior Developer** joining your first distributed microservices team or a **Senior/Staff Engineer** reviewing the cloud-native architecture, this guide will get you from `git clone` to running and testing the entire platform on your machine in under 10 minutes.

---

## 1. Prerequisites & Local Toolchain

Before building the project, verify that your workstation has the following installed:

| Tool | Required Version | Why We Use It | Check Command |
| :--- | :--- | :--- | :--- |
| **Java Development Kit (JDK)** | **Java 25 LTS** (e.g., Temurin 25.0.3+) | Powers Virtual Threads, `ScopedValue`, `StructuredTaskScope`, Stream `Gatherers`, and Generational ZGC. | `java -version` |
| **Gradle** | **Gradle 9.5+** (Included via `./gradlew`) | Builds the 10-module Groovy DSL monorepo using centralized version catalogs (`gradle/libs.versions.toml`). | `./gradlew -version` |
| **Docker & Docker Compose** | **Docker 24+** | Runs local PostgreSQL 17, Valkey/Redis 8, Apache Kafka (KRaft), ScyllaDB, and ClickHouse containers. | `docker compose version` |
| **PlantUML + Graphviz** *(Optional)* | Any recent version | Renders `.puml` architecture diagrams in `docs/architecture/` into `.svg` and `.png`. | `plantuml -version` |

---

## 2. Repository Map: Where Does Everything Live?

When you open the repository in IntelliJ IDEA or VS Code, you will see four main top-level areas:

```text
ariel-system/
├── buildSrc/                           # Shared Gradle Groovy build conventions (Java 25 + Spring Boot 4.1.2)
├── gradle/libs.versions.toml           # Single source of truth for all dependency versions
├── libs/                               # Reusable platform libraries shared by all microservices
│   ├── ariel-common-domain/            # Java 25 records, sealed interfaces, ScopedValue context
│   ├── ariel-observability-starter/    # Auto-configured correlation headers & RFC 9457 ProblemDetails
│   ├── ariel-cache-starter/            # Two-Tier Near-Cache (L1 Caffeine + L2 Redis) & Idempotency
│   └── ariel-outbox-starter/           # Transactional Outbox entities, publisher & Kafka relay
├── services/                           # The 6 independently deployable Spring Boot 4.1.2 microservices
│   ├── ariel-edge-gateway/             # Port 8080: API Gateway, Redis Lua Rate Limiter, Routing
│   ├── ariel-ingestion-service/        # Port 8081: Idempotency Barrier & Fast-Path Orchestrator
│   ├── ariel-risk-engine/              # Port 8082: Sub-5ms Parallel Fraud/Risk Decision Engine
│   ├── ariel-ledger-service/           # Port 8083: ACID Balance, Double-Entry Ledger & Outbox TX
│   ├── ariel-stream-processor/         # Port 8084: Kafka Consumer, Micro-Batching & Polyglot Sinks
│   └── ariel-query-service/            # Port 8085: CQRS Read-Only Analytics & Dashboard API
├── deploy/k8s/                         # Production Kubernetes Deployments, HPA & KEDA manifests
├── docs/                               # Architecture diagrams (.puml/.png/.svg) & Developer Guides
└── .agents/                            # AI coding agent personas, skills, rules, and prompts
```

---

## 3. Step-by-Step: Build & Run All Tests (Zero Docker Required!)

**Tip for Junior Developers**: Every unit and integration test in Project Ariel is designed to run **out-of-the-box without requiring external servers to be running first** (using embedded H2 in PostgreSQL compatibility mode and resilient in-memory fallbacks when Redis/Kafka are offline).

Run the full build and test suite from the project root:

```bash
./gradlew clean build
```

What happens when you run this command?
1. Gradle compiles `buildSrc` convention plugins ([ariel.java25-conventions.gradle](../../buildSrc/src/main/groovy/ariel.java25-conventions.gradle)).
2. Gradle compiles all 4 shared libraries (`libs/*`) and all 6 microservices (`services/*`) with Java 25 (`--enable-preview`).
3. JUnit 5 executes unit and integration tests across all modules using Generational ZGC (`-XX:+UseZGC`).
4. Spring Boot packages executable fat JARs (`bootJar`) for each of the 6 microservices.

To run tests for a single microservice while iterating on code:
```bash
./gradlew :services:ariel-risk-engine:test
```

---

## 4. Running the Full Cloud-Native Stack Locally

### Step 4.1: Start Local Infrastructure (Docker Compose)
To start PostgreSQL 17, Redis 8 (Valkey), Apache Kafka (KRaft), ScyllaDB, and ClickHouse:

```bash
docker compose up -d
```

Check that the containers are healthy:
```bash
docker compose ps
```

### Step 4.2: Start the Microservices
You can run the microservices in separate terminal tabs (or directly from your IDE's Spring Boot Run Dashboard):

```bash
# Terminal 1: Risk Engine (Port 8082)
./gradlew :services:ariel-risk-engine:bootRun

# Terminal 2: Transaction Ledger Service (Port 8083)
./gradlew :services:ariel-ledger-service:bootRun

# Terminal 3: Ingestion & Command Service (Port 8081)
./gradlew :services:ariel-ingestion-service:bootRun

# Terminal 4: CQRS Query & Analytics Service (Port 8085)
./gradlew :services:ariel-query-service:bootRun

# Terminal 5: Stream Processor Service (Port 8084)
./gradlew :services:ariel-stream-processor:bootRun

# Terminal 6: Edge Gateway (Port 8080)
./gradlew :services:ariel-edge-gateway:bootRun
```

> **Note**: Even if you don't start `docker compose up -d`, the services are coded with **graceful local fallbacks** (in-memory H2 PostgreSQL mode, Caffeine L1 cache, and local token buckets) so you can still boot and test the entire HTTP fast path on a laptop without Docker!

---

## 5. Making Your First API Calls

Once the services are running, run the included smoke & idempotency test script:

```bash
./tests/load-test.sh
```

Or execute `curl` commands manually against the **Edge Gateway (`http://localhost:8080`)**:

### A. Submit a New Transaction (`HTTP 202 Accepted`)
```bash
curl -i -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: tenant-acme" \
  -H "Idempotency-Key: order-checkout-2026-0001" \
  -d '{
    "accountId": "acc-customer-101",
    "counterpartyId": "merchant-store-99",
    "amount": 149.99,
    "currency": "USD",
    "region": "US-EAST",
    "deviceFingerprint": "ios-device-verified-01",
    "metadata": { "orderId": "ORD-9981" }
  }'
```

**Expected Response (`202 Accepted`)**:
```json
{
  "transactionId": "c4b1e8f0-9a12-4d3b-8e21-7a91bc0219ab",
  "correlationId": "8f19d02a-...",
  "tenantId": "tenant-acme",
  "accountId": "acc-customer-101",
  "amount": 149.99,
  "currency": "USD",
  "status": "APPROVED_AND_COMMITTED",
  "riskScore": 0.0415,
  "idempotentReplay": false,
  "processingMillis": 6,
  "processedAt": "2026-11-16T15:30:00.123Z"
}
```

### B. Retry the Exact Same Request (`HTTP 200 OK` with `idempotentReplay: true`)
Run the exact same `curl` command with the same `Idempotency-Key: order-checkout-2026-0001`.
Notice that:
- The HTTP status changes from `202 Accepted` to **`200 OK`**.
- `"idempotentReplay": true` is returned in `<1ms` from the cache.
- The customer's account balance in `ariel-ledger-service` is **NOT** charged a second time!

### C. Query Real-Time Tenant Analytics (`HTTP 200 OK`)
```bash
curl -s http://localhost:8080/api/v1/analytics/tenants/tenant-acme/summary \
  -H "X-Tenant-Id: tenant-acme" | jq .
```

### D. Check Health Probes & Prometheus Metrics
Every service exposes Kubernetes readiness/liveness probes and Prometheus metrics on its actuator port:
```bash
curl -s http://localhost:8081/actuator/health
curl -s http://localhost:8081/actuator/prometheus | head -n 30
```

---

## Next Steps in Your Reading Journey
- **Understand the Business Domain**: Read [02-business-domain-and-scenarios.md](02-business-domain-and-scenarios.md).
- **Learn the Java 25 & Spring Boot 4.1.2 Code**: Read [03-java25-springboot4-codebase-tour.md](03-java25-springboot4-codebase-tour.md).
- **Add Features & Maintain the System**: Read [04-extending-and-maintaining-guide.md](04-extending-and-maintaining-guide.md).
- **Understand System Design, Project Structure & ADRs**: Read [05-system-design-architecture-and-adrs.md](05-system-design-architecture-and-adrs.md).
- **Work with Unit, Integration, Load & Chaos Tests**: Read [06-performance-load-and-chaos-testing.md](06-performance-load-and-chaos-testing.md).
