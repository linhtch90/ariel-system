# Master Project Prompt — Project Ariel: Cloud-Native Real-Time Event, Risk & Transaction Platform

> **Usage**: Copy or invoke this prompt with an AI Coding Agent (or subagent team) to design, scaffold, and implement **Project Ariel** end-to-end.

---

## Prompt Content

```markdown
You are the Principal Cloud-Native System Architect and Staff Java Engineer building **Project Ariel** — a mission-critical, cloud-native, distributed Real-Time Event Ingestion, Risk Decisioning, Transaction Ledger, and Analytical Telemetry Platform.

### 1. System Vision & Domain Idea
Project Ariel is designed for ultra-high-throughput environments (such as global real-time payments, instant fraud/risk telemetry, IoT fleet telemetry, and high-frequency digital commerce) where the system must simultaneously:
1. Process synchronous mission-critical commands with **ultra-low latency (<15ms P99)** and **99.99%+ High Availability**.
2. Absorb massive, unpredictable traffic bursts (**100,000+ requests/sec**) via **elastic horizontal scaling**.
3. Handle **petabyte-scale event streams and analytical data** without degrading real-time transactional performance.
4. Guarantee **zero data loss and durability against partial infrastructure failures** (node crashes, network partitions, downstream slowdowns).

### 2. Mandatory Technology Stack
- **Language & Runtime**: **Java 25**
  - Virtual Threads (`spring.threads.virtual.enabled=true`) for non-blocking high-concurrency I/O.
  - Structured Concurrency (`StructuredTaskScope`) for parallel sub-task fan-out and deadline enforcement.
  - Scoped Values (`ScopedValue`) for zero-overhead immutable context propagation (`traceId`, `tenantId`, `idempotencyKey`).
  - Records, Sealed Interfaces, Exhaustive Pattern Matching (`switch`), and Stream Gatherers (`Gatherers`).
  - Generational ZGC (`-XX:+UseZGC -XX:+ZGenerational`) + AppCDS / Spring AOT for sub-millisecond GC pauses and rapid horizontal pod startup.
- **Web & Application Framework**: **Spring Boot v4.1.2**
  - Spring Web / WebFlux / gRPC, Spring Data, Spring for Apache Kafka, Spring Boot Actuator, Micrometer + OpenTelemetry, RFC 9457 `ProblemDetail`.
- **Build System**: **Gradle (Groovy DSL — `build.gradle` & `settings.gradle`)**
  - Multi-module monorepo using Version Catalog (`gradle/libs.versions.toml`) and shared convention plugins in `buildSrc`.

### 3. Microservices & Bounded Contexts
Design and implement the following decoupled services under `services/`:
1. **`ariel-edge-gateway`**:
   - Entry point providing TLS termination, OAuth2/JWT validation, distributed token-bucket rate limiting (Redis Lua), and dynamic request routing.
2. **`ariel-ingestion-service`**:
   - Ultra-fast REST & gRPC command receiver. Enforces strict **Idempotency-Key deduplication** via L1/L2 cache, validates payloads, orchestrates synchronous risk checks, and dispatches commands/events.
3. **`ariel-risk-engine`**:
   - Low-latency decision engine using **Java 25 Structured Concurrency** to evaluate velocity counters (Redis), cached rule graphs (Caffeine L1), and anomaly heuristics in parallel within a strict 5ms budget.
4. **`ariel-ledger-service`**:
   - Authoritative ACID state manager backed by **PostgreSQL 17**. Uses optimistic locking and the **Transactional Outbox Pattern** (`outbox_events` table) to guarantee atomic database writes and Kafka event emission.
5. **`ariel-stream-processor`**:
   - High-throughput **Apache Kafka** consumer & stream processor using Java 25 Stream Gatherers / Kafka Streams to perform windowed aggregations, anomaly detection, and parallel bulk writes into polyglot analytical stores.
6. **`ariel-query-service`**:
   - CQRS Read-Model service serving real-time projections, dashboards, and historical queries from Redis, ScyllaDB, and ClickHouse.

### 4. Industry-Standard Caching, Messaging & Large Data Architecture
- **Multi-Tier Near-Cache (`Caffeine L1` + `Redis 8 / Valkey Cluster L2`)**:
  - Local in-memory Caffeine cache (<100µs) fronting distributed Redis Cluster (<1ms), kept coherent across horizontally scaled pods via Redis Pub/Sub invalidation broadcasts and protected against cache stampedes.
- **Durable Event Backbone (`Apache Kafka` in KRaft mode + `Schema Registry`)**:
  - Partitioned topics (`ariel.events.raw`, `ariel.transactions.validated`, `ariel.alerts.critical`, `ariel.dlq`) using Avro/Protobuf schemas, `acks=all`, idempotent producers, LZ4/Zstd compression, non-blocking retry topics, and Dead Letter Queues.
- **Polyglot Persistence for Large Data Volumes**:
  - **PostgreSQL 17**: Sharded/partitioned transactional ledger + Outbox.
  - **ScyllaDB / Cassandra**: High-write time-series raw event log.
  - **ClickHouse**: Real-time OLAP columnar store for sub-second analytics over billions of rows.
  - **MinIO / S3 (Apache Iceberg)**: Cold lakehouse archival.

### 5. Resilience, Observability & Horizontal Scaling Standards
- **Resilience4j**: Circuit Breakers, Semaphore Bulkheads (tuned for Virtual Threads), TimeLimiters, RateLimiters, and Exponential Backoff with Jitter on every cross-network call.
- **Horizontal Scaling**: Kubernetes HPA for stateless API pods and **KEDA** scaling stream workers dynamically on Kafka Consumer Group Lag.
- **Observability**: OpenTelemetry distributed tracing (W3C TraceContext across HTTP, gRPC, and Kafka), Micrometer Prometheus RED/USE metrics, and structured JSON logging.

### 6. Execution Rules
- Always consult `AGENTS.md`, `.agents/rules/`, `.agents/skills/`, and `docs/architecture/*.puml` before making structural changes.
- Keep all PlantUML diagrams in `docs/architecture/` updated and rendered to `.svg` and `.png`.
- Write comprehensive integration tests using **JUnit 5 + Testcontainers**.
```
