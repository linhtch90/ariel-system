# AGENTS.md — Ariel Cloud-Native System

Welcome to **Project Ariel**, an enterprise-grade, ultra-low latency, high-throughput, cloud-native distributed event & transaction processing platform built on the modern Java stack.

This file serves as the root rulebook and architectural compass for all AI coding agents operating in this repository.

---

## 1. Core Technology Stack & Versions (Strict Enforcement)

- **Language**: **Java 25** (LTS / Modern Java features mandatory)
  - Use **Virtual Threads** (`spring.threads.virtual.enabled=true`) for high-throughput I/O operations.
  - Use **Structured Concurrency** (`StructuredTaskScope`) for parallel fan-out/fan-in workflows.
  - Use **Scoped Values** (`ScopedValue`) instead of `ThreadLocal` for propagating security, tenant, and tracing contexts across virtual threads.
  - Use **Records**, **Sealed Classes/Interfaces**, **Pattern Matching for `switch`**, **Record Patterns**, and **Stream Gatherers** (`Gatherers`).
  - JVM Runtime flags: **Generational ZGC** (`-XX:+UseZGC -XX:+ZGenerational`) for sub-millisecond GC pauses under heavy heap pressure, plus **CDS / AppCDS / Spring AOT** for fast pod startup.
- **Framework**: **Spring Boot v4.1.2** (Jakarta EE 11, Spring Framework 7.x baseline)
  - Declarative HTTP Interfaces (`@HttpExchange`), `RestClient`, gRPC, and Spring Modulith / Cloud-Native boundaries.
  - Built-in **ApiVersion** & **ProblemDetails (RFC 9457)** for standardized error responses.
  - **Spring Boot Actuator + Micrometer + OpenTelemetry** for zero-blind-spot observability.
- **Build Tool**: **Gradle (Groovy DSL — `build.gradle` / `settings.gradle`)**
  - Centralized dependency management via `gradle/libs.versions.toml` (Version Catalog) and custom convention plugins in `buildSrc` or `gradle/build-logic`.
  - Java Toolchain explicitly set to `JavaLanguageVersion.of(25)`.

---

## 2. Cloud-Native Non-Functional Requirements

1. **High Availability (99.99%+ SLA)**:
   - Stateless compute pods deployed across multiple Availability Zones (Multi-AZ).
   - Graceful shutdown (`server.shutdown=graceful`) with Kubernetes readiness/liveness/startup probes.
   - Zero-downtime deployments and schema migrations (Flyway / Liquibase expand-and-contract pattern).
2. **High Performance & Ultra-Low Latency (<15ms P99 Fast Path)**:
   - Two-tier caching strategy: **L1 In-Memory (Caffeine)** + **L2 Distributed (Redis 8 / Valkey Cluster)** with cache stampede protection and pub/sub invalidation.
   - Binary serialization (**Protobuf / Avro**) for internal service-to-service (gRPC) and event streaming (Kafka).
3. **Durability & Resilience to Failures**:
   - **Transactional Outbox Pattern + Debezium CDC** to guarantee atomic DB state updates and Kafka event publishing (zero dual-write bugs).
   - **Idempotency Keys** enforced at the Edge/Ingestion layer backed by Redis atomic locks.
   - **Resilience4j** Circuit Breakers, Bulkheads, Rate Limiters, TimeLimiters, and Exponential Backoff with Jitter on all remote calls.
   - **Dead Letter Queues (DLQ)** with automated replay and alerting for failed async messages.
4. **Elastic Horizontal Scaling & Massive Data Handling**:
   - **Kubernetes HPA + KEDA** scaling compute pods dynamically based on RPS, CPU/Memory, and **Kafka Consumer Lag**.
   - **Polyglot Persistence**:
     - **PostgreSQL 17 (Citus/Patroni)**: Sharded ACID transactional ledger & outbox.
     - **ScyllaDB / Apache Cassandra**: High-velocity write-heavy raw telemetry & time-series store.
     - **ClickHouse**: Real-time OLAP analytics over billions of events.
     - **Apache Iceberg on S3/MinIO**: Cold data lakehouse for historical batch/AI analytics.

---

## 3. Repository AI Customization Map

Before executing complex architectural or implementation tasks, consult the specialized AI assets in `.agents/`:

- **Rules**: [java25-springboot4-standards.md](.agents/rules/java25-springboot4-standards.md)
- **Prompts**:
  - [00-master-project-prompt.md](.agents/prompts/00-master-project-prompt.md)
  - [01-bootstrap-gradle-monorepo.md](.agents/prompts/01-bootstrap-gradle-monorepo.md)
  - [02-implement-core-services.md](.agents/prompts/02-implement-core-services.md)
  - [03-implement-data-and-streaming.md](.agents/prompts/03-implement-data-and-streaming.md)
  - [04-load-testing-and-chaos-engineering.md](.agents/prompts/04-load-testing-and-chaos-engineering.md)
- **Agent Personas**:
  - [cloud-architect.md](.agents/agents/cloud-architect.md)
  - [java-microservice-engineer.md](.agents/agents/java-microservice-engineer.md)
  - [data-streaming-specialist.md](.agents/agents/data-streaming-specialist.md)
  - [sre-resilience-auditor.md](.agents/agents/sre-resilience-auditor.md)
- **Skills**:
  - [scaffold-spring-service](.agents/skills/scaffold-spring-service/SKILL.md)
  - [implement-caching-messaging](.agents/skills/implement-caching-messaging/SKILL.md)
  - [resilience-and-scaling-audit](.agents/skills/resilience-and-scaling-audit/SKILL.md)
  - [plantuml-architecture-sync](.agents/skills/plantuml-architecture-sync/SKILL.md)
- **Architecture Diagrams**:
  - [system-architecture.puml](docs/architecture/system-architecture.puml)
  - [data-flow-sequence.puml](docs/architecture/data-flow-sequence.puml)
  - [scaling-resilience-topology.puml](docs/architecture/scaling-resilience-topology.puml)
