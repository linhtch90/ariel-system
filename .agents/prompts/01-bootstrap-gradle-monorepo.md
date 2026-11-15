# Phase 1 Prompt — Bootstrap Gradle Groovy Monorepo & Local Cloud Infrastructure

```markdown
Act as `java-microservice-engineer` and `cloud-architect`.
Follow the rules in `AGENTS.md` and `.agents/rules/java25-springboot4-standards.md`.

Task: Bootstrap the **Project Ariel** Gradle Groovy multi-module monorepo:
1. Create `settings.gradle` and root `build.gradle` (Groovy DSL) configured for **Java 25** toolchain and **Spring Boot v4.1.2**.
2. Create `gradle/libs.versions.toml` defining all library versions (Spring Boot 4.1.2, Resilience4j, Caffeine, Redisson/Lettuce, Spring Kafka, Protobuf/gRPC, Micrometer/OpenTelemetry, Testcontainers, ArchUnit).
3. Create shared libraries under `libs/`:
   - `libs/ariel-common-domain`: Shared Java 25 records (`EventId`, `TenantId`, `CorrelationContext`), `ScopedValue` holders, and sealed Result/Error types.
   - `libs/ariel-cache-starter`: Reusable Two-Tier Near-Cache (L1 Caffeine + L2 Redis + Pub/Sub invalidation).
   - `libs/ariel-outbox-starter`: Reusable Transactional Outbox entity, repository, and relay publisher.
   - `libs/ariel-observability-starter`: Auto-configuration for OpenTelemetry, Micrometer, and correlation filters.
4. Create `docker-compose.yml` for local cloud-native infrastructure:
   - PostgreSQL 17, Redis 8 (Valkey), Apache Kafka (KRaft) + Schema Registry, ScyllaDB, ClickHouse, and OpenTelemetry Collector + Grafana LGTM stack.
```
