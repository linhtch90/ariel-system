<div align="center">

# Project Ariel

### Cloud-Native Real-Time Financial Transaction, Risk Intelligence & Petabyte Telemetry Platform

[![Java 25](https://img.shields.io/badge/Java-25%20LTS-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/projects/jdk/25/)
[![Spring Boot 4.1.2](https://img.shields.io/badge/Spring%20Boot-4.1.2-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Gradle 9.5](https://img.shields.io/badge/Gradle-9.5%20Groovy-02303A?style=for-the-badge&logo=gradle&logoColor=white)](https://gradle.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft%203.9-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Redis 8 & Caffeine](https://img.shields.io/badge/Cache-Caffeine%20L1%20%2B%20Redis%208%20L2-DC382D?style=for-the-badge&logo=redis&logoColor=white)](libs/ariel-cache-starter)
[![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL-17%20ACID%20%2B%20Outbox-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](services/ariel-ledger-service)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)](LICENSE)

[**Business Context**](#1-what-is-this-system-about-business--engineering-context) •
[**Architecture Diagrams**](#2-system-architecture-diagrams-plantuml) •
[**Microservices & Libraries**](#3-microservices--shared-platform-libraries) •
[**Streaming vs. Batch**](#4-why-data-streaming-instead-of-batch-processing) •
[**Tech Choices (ADRs)**](#5-why-we-chose-this-stack-summary-of-adrs) •
[**Quick Start**](#6-getting-started-build-test--run-in-5-minutes) •
[**Developer Guides**](#7-developer--business-documentation-hub-junior-to-senior-path)

</div>

---

## At-a-Glance Engineering Highlights

**Project Ariel** is a production-grade reference cloud-native distributed system built on **Java 25**, **Spring Boot v4.1.2**, and **Gradle 9.5 (Groovy DSL)**. It demonstrates how modern Java concurrency and cloud-native patterns work together to achieve extreme throughput, sub-15ms latency, and zero data loss:

| Cloud-Native Pillar | Target SLO / Metric | Architectural Pattern | Codebase Implementation |
| :--- | :--- | :--- | :--- |
| **Ultra-Low Latency** | **`< 15ms` P99** (Fast Path)<br>**`< 2ms` P99** (Replay) | **Two-Tier Near-Cache** (`L1 Caffeine <100µs` + `L2 Redis 8 <1ms`) + Single-Flight Stampede Coalescing + Pub/Sub Invalidation | [`TwoTierNearCache.java`](libs/ariel-cache-starter/src/main/java/io/ariel/cache/TwoTierNearCache.java) |
| **Parallel Concurrency** | **`< 5ms` Risk Decision** | **Java 25 Virtual Threads**, **`StructuredTaskScope`** (parallel fan-out with deadline cancellation), and **`ScopedValue`** context propagation | [`ParallelRiskEvaluationService.java`](services/ariel-risk-engine/src/main/java/io/ariel/risk/service/ParallelRiskEvaluationService.java)<br>[`RequestContextHolder.java`](libs/ariel-common-domain/src/main/java/io/ariel/common/context/RequestContextHolder.java) |
| **Zero Double-Charge** | **100% Idempotent** | Atomic **Redis Lua `SET NX PX` Idempotency Barrier** at the Ingestion edge with 24-hour receipt caching | [`RedisIdempotencyService.java`](libs/ariel-cache-starter/src/main/java/io/ariel/cache/RedisIdempotencyService.java) |
| **Zero Data Loss (`RPO=0`)** | **Zero Dual-Write Bugs** | **Transactional Outbox Pattern** (`Propagation.MANDATORY` in PostgreSQL 17) + `@Version` Optimistic Locking + Kafka `acks=all` Relay | [`TransactionalOutboxPublisher.java`](libs/ariel-outbox-starter/src/main/java/io/ariel/outbox/TransactionalOutboxPublisher.java)<br>[`LedgerCommandService.java`](services/ariel-ledger-service/src/main/java/io/ariel/ledger/service/LedgerCommandService.java) |
| **Massive Data Streaming** | **Millions of Events/sec** | **Java 25 Stream Gatherers (`Gatherers.windowFixed(100)`)** micro-batching + parallel writes to **ScyllaDB**, **ClickHouse**, and **Iceberg** + **`ariel.dlq`** isolation | [`ValidatedTransactionStreamProcessor.java`](services/ariel-stream-processor/src/main/java/io/ariel/stream/processor/ValidatedTransactionStreamProcessor.java) |
| **Fault Resilience & Scale** | **`99.99%+` Availability**<br>**`3 -> 100+` Pods** | **Resilience4j** (`SemaphoreBulkhead`, `CircuitBreaker`), **Redis Lua Token-Bucket Rate Limiter**, **Kubernetes HPA + KEDA** | [`IngestionOrchestrationUseCase.java`](services/ariel-ingestion-service/src/main/java/io/ariel/ingestion/usecase/IngestionOrchestrationUseCase.java)<br>[`RedisTokenBucketRateLimiter.java`](services/ariel-edge-gateway/src/main/java/io/ariel/gateway/ratelimit/RedisTokenBucketRateLimiter.java) |

---

## 1. What Is This System About? (Business & Engineering Context)

**Project Ariel** models the core transaction, fraud/risk decisioning, and real-time analytical engine behind modern fintech and digital commerce platforms (such as **Stripe**, **Adyen**, or **Grab/Uber's payment & risk platform**).

Imagine millions of mobile checkouts, payment terminals, and partner gateways submitting financial transactions and telemetry events every second—including sudden **100x traffic spikes** during flash sales. For every request, the system must solve two competing problems simultaneously:

1. **The Synchronous "Fast Path" (`< 15ms P99`)**:
   - Authenticate the tenant and enforce **token-bucket rate limits** so a noisy tenant cannot degrade the platform.
   - Deduplicate network retries via the **`Idempotency-Key` barrier** so a customer tapping "Pay" twice in a subway tunnel is never double-charged.
   - Evaluate **3 fraud & compliance checks in parallel within `5ms`** (Account Spending Velocity, Tenant Rule Graphs, and Anomaly Heuristics) before any money moves.
   - If approved, update the account balance, write an immutable accounting ledger entry, and stage an Outbox event in a **single ACID PostgreSQL transaction**.
2. **The Asynchronous "Big Data & Event Path" (Petabyte Scale)**:
   - Stream every committed transaction through **Apache Kafka (KRaft)** into purpose-built analytical stores (**ScyllaDB** for high-velocity time-series audit trails, **ClickHouse** for sub-second OLAP queries over billions of rows, and **Apache Iceberg** for cold lakehouse batch/ML)—without ever slowing down live customer checkouts.

---

## 2. System Architecture Diagrams (PlantUML)

All architecture diagrams are maintained as version-controlled PlantUML code in [`docs/architecture/`](docs/architecture/) and compiled to both `.svg` and `.png`:

### 2.1 End-to-End Cloud-Native System Architecture
> *Source: [`system-architecture.puml`](docs/architecture/system-architecture.puml) | Rendered: [`SVG`](docs/architecture/system-architecture.svg) • [`PNG`](docs/architecture/system-architecture.png)*

![System Architecture](docs/architecture/system-architecture.svg)

---

### 2.2 Low-Latency Fast Path & Durable Event Streaming Sequence
> *Source: [`data-flow-sequence.puml`](docs/architecture/data-flow-sequence.puml) | Rendered: [`SVG`](docs/architecture/data-flow-sequence.svg) • [`PNG`](docs/architecture/data-flow-sequence.png)*

![Data Flow Sequence](docs/architecture/data-flow-sequence.svg)

---

### 2.3 Elastic Horizontal Scaling (HPA + KEDA), Resilience4j & HA Topology
> *Source: [`scaling-resilience-topology.puml`](docs/architecture/scaling-resilience-topology.puml) | Rendered: [`SVG`](docs/architecture/scaling-resilience-topology.svg) • [`PNG`](docs/architecture/scaling-resilience-topology.png)*

![Scaling & Resilience Topology](docs/architecture/scaling-resilience-topology.svg)

---

## 3. Microservices & Shared Platform Libraries

### 3.1 The 6 Cloud-Native Microservices (`services/*`)

| Microservice | Port | Business Role | Key Technical Responsibilities |
| :--- | :---: | :--- | :--- |
| **[`ariel-edge-gateway`](services/ariel-edge-gateway)** | `8080` | **Front Door & Traffic Shield** | Enforces per-tenant **atomic Redis Lua token-bucket rate limiting** (`X-RateLimit-Remaining`, `HTTP 429`) and routes traffic via Resilience4j circuit-broken `@HttpExchange` clients. |
| **[`ariel-ingestion-service`](services/ariel-ingestion-service)** | `8081` | **Frontline Command Coordinator** | Enforces **Idempotency-Key deduplication** (`<1ms` via L1/L2 cache), orchestrates `Risk Engine -> Ledger` over **Java 25 Virtual Threads**, and uses exhaustive pattern matching over `sealed interface RiskDecision`. |
| **[`ariel-risk-engine`](services/ariel-risk-engine)** | `8082` | **Instant Fraud & Rule Judge** | Uses **Java 25 `StructuredTaskScope`** to execute 3 checks concurrently in `<5ms`: (1) Redis spending velocity, (2) Caffeine L1 tenant rules, and (3) anomaly heuristics. Returns `Approved`, `ManualReviewRequired`, or `Rejected`. |
| **[`ariel-ledger-service`](services/ariel-ledger-service)** | `8083` | **ACID System of Record** | Updates `account_balances` (`@Version` optimistic locking), records immutable `ledger_entries` in **PostgreSQL 17** (Flyway-managed), and stages `OutboxEventEntity` records atomically in the same ACID transaction. |
| **[`ariel-stream-processor`](services/ariel-stream-processor)** | `8084` | **Big Data Pipeline Worker** | Consumes from **Apache Kafka**, partitions batches via **Java 25 `Stream.gather(Gatherers.windowFixed(100))`**, fans out parallel bulk writes to **ScyllaDB**, **ClickHouse**, and **Redis**, and isolates poison pills into **`ariel.dlq`**. |
| **[`ariel-query-service`](services/ariel-query-service)** | `8085` | **CQRS Read & Analytics API** | Serves sub-second merchant revenue summaries, risk KPIs, and telemetry projections from ClickHouse, ScyllaDB, and the Two-Tier Near-Cache—completely isolated from the write database. |

### 3.2 The 4 Shared Platform Libraries (`libs/*`)

| Library Module | Purpose |
| :--- | :--- |
| **[`libs:ariel-common-domain`](libs/ariel-common-domain)** | Pure Java 25 `record` DTOs (`TransactionCommand`, `TransactionReceipt`, `ValidatedTransactionEvent`), `sealed interface RiskDecision`, `ScopedValue<RequestContext>`, and domain exceptions. |
| **[`libs:ariel-observability-starter`](libs/ariel-observability-starter)** | Auto-configured Spring Boot starter that binds `X-Correlation-Id`, `X-Tenant-Id`, and `Idempotency-Key` into Java 25 `ScopedValue`, propagates headers to outbound `RestClient` calls, and formats errors as **RFC 9457 `ProblemDetail`**. |
| **[`libs:ariel-cache-starter`](libs/ariel-cache-starter)** | Provides `TwoTierNearCache` (**L1 Caffeine** + **L2 Redis 8** + single-flight `CompletableFuture` stampede protection + Redis Pub/Sub invalidation) and `RedisIdempotencyService`. |
| **[`libs:ariel-outbox-starter`](libs/ariel-outbox-starter)** | Provides `OutboxEventEntity`, `TransactionalOutboxPublisher` (`Propagation.MANDATORY`), and `OutboxRelayWorker` for zero-dual-write Kafka event publishing. |

---

## 4. Why Data Streaming Instead of Batch Processing?

A critical system design question is: *Why use real-time data streaming (**Apache Kafka + Stream Processor**) instead of traditional **Batch Processing** (running an hourly or nightly ETL job)?*

1. **Protecting the `<15ms` Fast Write Path**:
   Every transaction ultimately updates 4 stores: **PostgreSQL** (ledger), **Redis** (velocity counters), **ScyllaDB** (audit/telemetry log), and **ClickHouse** (analytics). Writing to all 4 synchronously during checkout would push latency over `150ms+` and couple checkout availability to analytical databases. Streaming decouples them so checkout commits only to PostgreSQL (`Ledger + Outbox`) in `<15ms`.
2. **Shock Absorber During 100x Traffic Spikes**:
   When traffic spikes from `5,000 RPS` to `100,000 RPS`, **Apache Kafka** buffers the burst on disk while **KEDA** autoscales `ariel-stream-processor` pods (`3 -> 64`) based on consumer lag—with **zero dropped events**.

| Dimension | Traditional Batch Processing (Hourly / Nightly ETL) | Real-Time Data Streaming (Apache Kafka + Stream Processor) |
| :--- | :--- | :--- |
| **Fraud & Risk Detection** | **Too Late**: Velocity counters update hours later; attackers drain accounts before the batch job starts. | **Sub-Second (`<100ms`)**: Rolling velocity counters update immediately; fraudulent bursts are blocked on the next request. |
| **Database Load Pattern** | **Spiky "Batch Storms"**: Massive hourly `SELECT` scans spike PostgreSQL CPU/IO to 100%, slowing down live users. | **Smooth Continuous Flow**: Events trickle continuously via Outbox Relay with negligible database overhead. |
| **Live Dashboards & Alerts** | **Stale Data**: Dashboards show metrics that are hours old. | **Real-Time Visibility**: ClickHouse and Grafana reflect live revenue and P99 risk scores within `~500ms`. |
| **Failure Recovery** | **Coarse Reruns**: If a 2-hour batch job fails at 95%, teams must reconcile millions of rows. | **Granular Offsets & DLQ**: Individual malformed events route to `ariel.dlq` while the remaining 99.999% continue uninterrupted. |

> **Hybrid Streaming + Micro-Batching**: Because columnar/wide-column stores (**ClickHouse** & **ScyllaDB**) prefer batch inserts over single-row writes, `ariel-stream-processor` uses **Java 25 `Stream.gather(Gatherers.windowFixed(100))`** to group continuous stream events into 100-event micro-batches—delivering **real-time freshness (`<200ms`)** with **batch-level write throughput**, while archiving cold history to **Apache Iceberg** for true batch ML training.

---

## 5. Why We Chose This Stack (Summary of ADRs)

For full architectural trade-offs, see **[Guide 05 — System Design, Project Structure & ADRs](docs/developer-guide/05-system-design-architecture-and-adrs.md)**:

| Decision | Chosen Technology | Why Chosen Over Alternatives? |
| :--- | :--- | :--- |
| **ADR-001** | **Java 25 Virtual Threads + `StructuredTaskScope` + `ScopedValue`** | Achieves the massive I/O scalability of Reactive WebFlux (`Mono`/`Flux`) while preserving simple imperative code, readable stack traces, and zero-leak context propagation. |
| **ADR-002** | **Spring Boot 4.1.2 `@HttpExchange` (`RestClient`)** | First-class Spring Framework 7 declarative HTTP client; eliminates third-party OpenFeign dependencies and integrates natively with Virtual Threads and `ClientHttpRequestInitializer`. |
| **ADR-003** | **Gradle 9.5 (Groovy DSL + `buildSrc` + Version Catalog)** | Centralizes Java 25 toolchain, `--enable-preview`, and `-XX:+UseZGC` flags in `buildSrc`, keeping service `build.gradle` files under 15 lines. |
| **ADR-004** | **Two-Tier Near-Cache (`Caffeine L1` + `Redis 8 L2`)** | Offloads 90%+ of hot tenant-rule reads to pod-local memory (`<100µs`) while keeping pods coherent via Redis Pub/Sub invalidation and single-flight stampede protection. |
| **ADR-005** | **Apache Kafka (KRaft Mode)** | Append-only log handles millions of events/sec, supports multi-consumer replay, and pairs with **KEDA** for partition-lag autoscaling (unlike point-to-point RabbitMQ queues). |
| **ADR-006** | **Transactional Outbox Pattern** | Guarantees atomic PostgreSQL ledger commits and Kafka event emission (`Propagation.MANDATORY`) without slow distributed 2-Phase Commit (2PC/XA) locks. |
| **ADR-007** | **Polyglot Persistence (`Postgres 17` + `ScyllaDB` + `ClickHouse` + `Iceberg`)** | Uses purpose-built engines: PostgreSQL for ACID locks, ScyllaDB for write-heavy time-series, ClickHouse for billion-row OLAP scans, and Iceberg for cold lakehouse storage. |
| **ADR-008** | **Dual Autoscaling (`Kubernetes HPA` + `KEDA`)** | Scales stateless API pods on RPS/CPU (`HPA`) while scaling I/O-bound stream worker pods directly on **Kafka Consumer Lag** (`KEDA`). |

---

## 6. Getting Started (Build, Test & Run in 5 Minutes)

### 6.1 Prerequisites
* **Java 25 LTS** (`java -version` $\rightarrow$ OpenJDK 25+)
* **Docker & Docker Compose** *(Optional for unit/integration tests; used for running the full local polyglot stack)*

### 6.2 Build & Run All Tests Out-of-the-Box
All unit and integration tests run out-of-the-box with zero external setup required:

```bash
./gradlew clean build
```

### 6.3 Start Local Infrastructure & Microservices
```bash
# 1. Start PostgreSQL 17, Valkey/Redis 8, Apache Kafka (KRaft), ScyllaDB, and ClickHouse:
docker compose up -d

# 2. Boot the microservices (in separate terminals or via your IDE):
./gradlew :services:ariel-risk-engine:bootRun
./gradlew :services:ariel-ledger-service:bootRun
./gradlew :services:ariel-ingestion-service:bootRun
./gradlew :services:ariel-query-service:bootRun
./gradlew :services:ariel-edge-gateway:bootRun

# 4. Run the automated 10,000-request Virtual Thread Load, Limits & Chaos Engineering suite:
./gradlew :tests:ariel-load-and-chaos-tests:test

# 5. Run the smoke & idempotency verification script (or Grafana k6 autoscaling spike test):
./tests/load-test.sh
# k6 run tests/k6/ariel-scale-and-chaos.js
```

### 6.4 Sample API Request & Response

**Request (`POST http://localhost:8080/api/v1/transactions`)**:
```bash
curl -i -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: tenant-acme" \
  -H "Idempotency-Key: checkout-2026-0001" \
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

**Response (`HTTP 202 Accepted` on first call, `HTTP 200 OK` with `"idempotentReplay": true` on retry)**:
```json
{
  "transactionId": "c4b1e8f0-9a12-4d3b-8e21-7a91bc0219ab",
  "correlationId": "8f19d02a-4c11-4b90-9d1a-3b7e2f8a1029",
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

---

## 7. Developer & Business Documentation Hub (Junior-to-Senior Path)

We maintain a complete documentation suite in [`docs/`](docs/) designed so **Junior Developers**, **Product/Domain Engineers**, and **Senior/Staff Architects** can onboard, understand, maintain, and extend the system effortlessly:

| Guide | Target Audience | What You Will Learn |
| :--- | :--- | :--- |
| **[01 — Onboarding & Quick-Start Guide](docs/developer-guide/01-onboarding-and-quickstart.md)** | **All Developers (Day 1)** | Toolchain setup, repository map, running `./gradlew clean build`, starting Docker Compose & the 6 microservices, and testing APIs & actuator probes. |
| **[02 — Business Domain, Invariants & Scenarios](docs/developer-guide/02-business-domain-and-scenarios.md)** | **Junior $\rightarrow$ Senior & Product** | The B2B2C financial & risk business model, domain glossary, the **4 Non-Negotiable Financial Invariants**, and **5 real-world user scenarios** traced line-by-line through the code. |
| **[03 — Java 25 & Spring Boot 4.1.2 Codebase Tour](docs/developer-guide/03-java25-springboot4-codebase-tour.md)** | **Junior $\rightarrow$ Staff Engineers** | Side-by-side comparisons of legacy Java vs. **Java 25 (`Virtual Threads`, `ScopedValue`, `StructuredTaskScope`, `Gatherers`, `sealed interface`)** and **Spring Boot 4.1.2 (`@HttpExchange`, `TwoTierNearCache`, `TransactionalOutbox`)**. |
| **[04 — Extending, Maintaining & Troubleshooting Playbook](docs/developer-guide/04-extending-and-maintaining-guide.md)** | **Mid-Level $\rightarrow$ Senior / SRE** | Step-by-step recipes for adding risk checks, endpoints, zero-downtime Flyway migrations, new microservices, and troubleshooting `429`/`409`/`503`/DLQ events in production. |
| **[05 — System Design, Project Structure, Architecture & ADRs](docs/developer-guide/05-system-design-architecture-and-adrs.md)** | **All Levels (Junior $\rightarrow$ Staff)** | Capacity math (`<15ms` latency budget breakdown), why the monorepo & packages are structured this way, CQRS/Outbox/Near-Cache architecture, and **8 Architecture Decision Records (ADRs)**. |
| **[06 — Performance, High-Load, System Limits & Chaos Testing](docs/developer-guide/06-performance-load-and-chaos-testing.md)** | **All Levels & SREs** | How to run the **10,000-request Virtual Thread latency benchmark (`23,000+ RPS`, `<15ms P99`)**, noisy-neighbor rate-limit & bulkhead saturation tests, **4 Chaos Engineering fault-injection experiments**, and **Grafana k6 HPA/KEDA autoscaling spike tests**. |
| **[Architecture Deep-Dive](docs/architecture/architecture-deep-dive.md)** | **Architects & Staff Engineers** | Detailed service boundary analysis, hybrid streaming/micro-batch/lakehouse architecture, and polyglot persistence matrix. |
| **[Implementation Plan](docs/implementation_plan.md)** | **All Engineers** | The 4-phase technical specification and module dependency graph. |

---

## 8. Repository & AI-Native Project Layout

This repository combines a 10-module Gradle Groovy monorepo (`libs/` + `services/`) with first-class AI coding agent customizations (`.agents/` & [`AGENTS.md`](AGENTS.md)):

```text
ariel-system/
├── AGENTS.md                                              # Root AI Agent Constitution & Architecture Rules
├── README.md                                              # Project Overview, Business Context & Documentation Hub
├── LICENSE                                                # MIT License
├── settings.gradle / build.gradle                         # Root Gradle 9.5 Groovy DSL Multi-Project Configuration
├── gradle/libs.versions.toml                              # Centralized Version Catalog (Spring Boot 4.1.2, Java 25)
├── buildSrc/                                              # Shared Gradle Groovy Convention Plugins
│   └── src/main/groovy/
│       ├── ariel.java25-conventions.gradle                # Java 25 toolchain, --enable-preview, ZGC, JUnit Platform
│       └── ariel.spring-boot-service.gradle               # Spring Boot 4.1.2 service conventions & actuator setup
├── libs/                                                  # Shared Platform Libraries
│   ├── ariel-common-domain/                               # Java 25 records, sealed RiskDecision, ScopedValue context
│   ├── ariel-observability-starter/                       # Auto-configured correlation filter & RFC 9457 ProblemDetails
│   ├── ariel-cache-starter/                               # L1 Caffeine + L2 Redis Near-Cache & Lua Idempotency
│   └── ariel-outbox-starter/                              # Transactional Outbox entity, publisher & Kafka relay
├── services/                                              # Cloud-Native Microservices
│   ├── ariel-edge-gateway/                                # Port 8080: API Gateway, Redis Lua Rate Limiter & Routing
│   ├── ariel-ingestion-service/                           # Port 8081: Idempotency Barrier & Fast-Path Orchestrator
│   ├── ariel-risk-engine/                                 # Port 8082: Sub-5ms Parallel StructuredTaskScope Risk Engine
│   ├── ariel-ledger-service/                              # Port 8083: ACID Balance, Ledger & Transactional Outbox
│   ├── ariel-stream-processor/                            # Port 8084: Kafka Consumer, Gatherers & Polyglot Sinks
│   └── ariel-query-service/                               # Port 8085: CQRS Read-Model Analytics & Dashboard API
├── deploy/k8s/                                            # Multi-AZ Kubernetes Deployments, HPA & KEDA Scalers
├── docker-compose.yml                                     # Local Polyglot Infrastructure (Postgres, Redis, Kafka, etc.)
├── tests/                                                 # Load, Stress, System Limits & Chaos Engineering Suites
│   ├── ariel-load-and-chaos-tests/                        # Automated Java 25 High-Load (10k reqs), Limit & Chaos Tests
│   ├── k6/ariel-scale-and-chaos.js                        # Grafana k6 4-Stage HPA & KEDA Autoscaling Spike Script
│   └── load-test.sh                                       # End-to-End Fast-Path & Idempotency Verification Script
├── .agents/                                               # AI Subagent Personas, Procedural Skills, Rules & Prompts
└── docs/
    ├── implementation_plan.md                             # Detailed 4-Phase Technical Implementation Plan
    ├── developer-guide/                                   # Junior-to-Senior Onboarding, Business & Extension Guides
    │   ├── 01-onboarding-and-quickstart.md
    │   ├── 02-business-domain-and-scenarios.md
    │   ├── 03-java25-springboot4-codebase-tour.md
    │   ├── 04-extending-and-maintaining-guide.md
    │   ├── 05-system-design-architecture-and-adrs.md
    │   └── 06-performance-load-and-chaos-testing.md
    └── architecture/                                      # Architecture Deep-Dive + PlantUML (.puml/.svg/.png)
        ├── architecture-deep-dive.md
        ├── system-architecture.puml
        ├── data-flow-sequence.puml
        └── scaling-resilience-topology.puml
```

---

## 9. License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.
