# AGENTS.md — Ariel Cloud-Native System

Welcome to **Project Ariel**, an enterprise-grade, ultra-low latency, high-throughput, cloud-native distributed event & transaction processing platform built on the modern Java stack.

This file serves as the root rulebook and architectural compass for all AI coding agents operating in this repository.

---

## 1. Core Technology Stack & Versions (Strict Enforcement)

- **Language**: **Java 25 LTS** (`OpenJDK 25.0.3+`, `--enable-preview` enabled via `buildSrc`)
  - Use **Virtual Threads** (`spring.threads.virtual.enabled=true`) for high-throughput I/O operations.
  - Use **Structured Concurrency** (`StructuredTaskScope.open(Joiner.awaitAllSuccessfulOrThrow(), cf -> cf.withTimeout(...))`) for parallel fan-out/fan-in workflows.
  - Use **Scoped Values** (`ScopedValue<RequestContext>`) instead of `ThreadLocal` for propagating correlation, tenant, and idempotency contexts across virtual threads.
  - Use **Records**, **Sealed Classes/Interfaces**, **Exhaustive Pattern Matching for `switch`**, **Unnamed Variables (`_`)**, and **Stream Gatherers** (`Gatherers.windowFixed`).
  - JVM Runtime flags: **Generational ZGC** (`-XX:+UseZGC` — generational by default in Java 25) for sub-millisecond GC pauses under heavy heap pressure.
- **Framework**: **Spring Boot v4.1.2** (Jakarta EE 11, Spring Framework 7.0, Jackson 3 `tools.jackson.databind.json.JsonMapper`)
  - Modular starters (`spring-boot-starter-webmvc`, `spring-boot-starter-restclient`, `spring-boot-starter-flyway`, `spring-boot-starter-opentelemetry`).
  - Declarative HTTP Interfaces (`@HttpExchange`), `RestClient`, and `ClientHttpRequestInitializer` context propagation.
  - Standardized **ProblemDetails (RFC 9457)** via `GlobalProblemDetailExceptionHandler`.
- **Build Tool**: **Gradle 9.5 (Groovy DSL — `build.gradle` / `settings.gradle`)**
  - Centralized dependency management via `gradle/libs.versions.toml` and convention plugins in `buildSrc` (`ariel.java25-conventions` and `ariel.spring-boot-service`).
  - Java Toolchain explicitly set to `JavaLanguageVersion.of(25)` with `testRuntimeOnly 'org.junit.platform:junit-platform-launcher'`.

---

## 2. Cloud-Native Non-Functional Requirements & Verified SLOs

1. **High Availability (99.99%+ SLA)**:
   - Stateless compute pods deployed across multiple Availability Zones (Multi-AZ).
   - Graceful shutdown (`server.shutdown=graceful`) with Kubernetes readiness/liveness/startup probes.
   - Zero-downtime schema migrations (Flyway expand-and-contract pattern).
2. **High Performance & Ultra-Low Latency (<15ms P99 Fast Path — Benchmarked at ~23,000 RPS / ~13.6ms P99)**:
   - Two-tier near-caching: **L1 In-Memory (Caffeine, `<100µs`)** + **L2 Distributed (Redis 8 / Valkey, `<1ms`)** with single-flight `CompletableFuture` stampede protection and Redis Pub/Sub invalidation (`TwoTierNearCache`).
3. **Durability & Resilience to Failures**:
   - **Transactional Outbox Pattern (`Propagation.MANDATORY`) + Scheduled Relay** (`TransactionalOutboxPublisher`, `OutboxRelayWorker`) to guarantee atomic DB state updates and Kafka event publishing (zero dual-write bugs).
   - **Idempotency Keys** enforced at the Ingestion layer via atomic Redis Lua locks (`RedisIdempotencyService`).
   - **Resilience4j** `CircuitBreaker`, `SemaphoreBulkhead` (tuned for Virtual Threads), and `StructuredTaskScope` deadline cancellation on remote calls.
   - **Dead Letter Queues (`ariel.dlq`)** for poison-pill isolation in `ValidatedTransactionStreamProcessor`.
4. **Elastic Horizontal Scaling & Massive Data Handling**:
   - **Kubernetes HPA + KEDA** scaling API pods (`3 -> 100+`) on RPS/CPU and stream worker pods (`3 -> 64`) on **Kafka Consumer Lag**.
   - **Polyglot Persistence**: PostgreSQL 17 (ACID ledger & outbox), Redis 8 (L2 cache, velocity counters, Lua token buckets), ScyllaDB (time-series telemetry), ClickHouse (real-time OLAP), and Apache Iceberg (cold lakehouse).

---

## 3. Repository AI Customization & Documentation Map

Before executing architectural, implementation, or testing tasks, consult the specialized assets in `.agents/` and `docs/`:

- **Rules**: [java25-springboot4-standards.md](.agents/rules/java25-springboot4-standards.md)
- **Skills**:
  - [scaffold-spring-service](.agents/skills/scaffold-spring-service/SKILL.md)
  - [implement-caching-messaging](.agents/skills/implement-caching-messaging/SKILL.md)
  - [resilience-and-scaling-audit](.agents/skills/resilience-and-scaling-audit/SKILL.md)
  - [plantuml-architecture-sync](.agents/skills/plantuml-architecture-sync/SKILL.md)
  - [run-and-maintain-tests](.agents/skills/run-and-maintain-tests/SKILL.md)
- **Agent Personas**:
  - [cloud-architect.md](.agents/agents/cloud-architect.md)
  - [java-microservice-engineer.md](.agents/agents/java-microservice-engineer.md)
  - [data-streaming-specialist.md](.agents/agents/data-streaming-specialist.md)
  - [sre-resilience-auditor.md](.agents/agents/sre-resilience-auditor.md)
- **Prompts**:
  - [00-master-project-prompt.md](.agents/prompts/00-master-project-prompt.md)
  - [01-bootstrap-gradle-monorepo.md](.agents/prompts/01-bootstrap-gradle-monorepo.md)
  - [02-implement-core-services.md](.agents/prompts/02-implement-core-services.md)
  - [03-implement-data-and-streaming.md](.agents/prompts/03-implement-data-and-streaming.md)
  - [04-load-testing-and-chaos-engineering.md](.agents/prompts/04-load-testing-and-chaos-engineering.md)
- **Developer & Architecture Guides**:
  - [01-onboarding-and-quickstart.md](docs/developer-guide/01-onboarding-and-quickstart.md)
  - [02-business-domain-and-scenarios.md](docs/developer-guide/02-business-domain-and-scenarios.md)
  - [03-java25-springboot4-codebase-tour.md](docs/developer-guide/03-java25-springboot4-codebase-tour.md)
  - [04-extending-and-maintaining-guide.md](docs/developer-guide/04-extending-and-maintaining-guide.md)
  - [05-system-design-architecture-and-adrs.md](docs/developer-guide/05-system-design-architecture-and-adrs.md)
  - [06-performance-load-and-chaos-testing.md](docs/developer-guide/06-performance-load-and-chaos-testing.md)
  - [architecture-deep-dive.md](docs/architecture/architecture-deep-dive.md)
  - [implementation_plan.md](docs/implementation_plan.md)
