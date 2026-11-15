---
name: implement-caching-messaging
description: >-
  Use this skill when implementing multi-tier caching (L1 Caffeine + L2 Redis)
  or durable event streaming (Apache Kafka + Transactional Outbox) in Ariel services.
---

# Skill: Implement Multi-Tier Caching & Durable Event Messaging

Use this runbook to implement industry-standard caching and messaging patterns in the Ariel System.

## Part A: Two-Tier Near-Cache (L1 Caffeine + L2 Redis Cluster)

1. **Read Flow (Cache-Aside with Near-Cache)**:
   - Query **L1 (Caffeine)** in-memory cache first (`<100µs`).
   - On L1 miss, query **L2 (Redis 8 / Valkey)** distributed cache (`<1ms`).
   - On L2 miss, acquire a short-lived distributed lock or single-flight coalescing (`CompletableFuture` in-flight map) to prevent **cache stampede**, load from the primary datastore, populate L2 with TTL + jitter, and populate L1.
2. **Invalidation Flow**:
   - On write/update, update or evict L2 Redis key and publish an invalidation message to the Redis Pub/Sub channel `ariel:cache:invalidation`.
   - All horizontally scaled pods subscribe to `ariel:cache:invalidation` and immediately evict the stale key from their local L1 Caffeine instance.
3. **Idempotency Barrier**:
   - Use atomic Redis Lua script (`SET key IN_PROGRESS NX EX 60`) at the ingestion edge to deduplicate concurrent retries before executing business logic.

## Part B: Durable Kafka Messaging & Transactional Outbox

1. **Zero Dual-Write Guarantee (Transactional Outbox)**:
   - Never publish directly to Kafka inside a `@Transactional` database method.
   - Persist both the domain entity mutation and an `OutboxEventRecord` (UUID v7, aggregateType, aggregateId, eventType, Avro/JSON payload, traceHeaders, createdAt) in the **same PostgreSQL ACID transaction**.
   - Stream `outbox_events` to Apache Kafka via **Debezium CDC** or a high-efficiency **SKIP LOCKED Outbox Relay** polling worker.
2. **High-Throughput Kafka Producer Settings**:
   - `acks: all`
   - `enable.idempotence: true`
   - `compression.type: lz4` (or `zstd`)
   - `linger.ms: 5`
   - `batch.size: 65536`
3. **Resilient Consumer Group & Dead Letter Queue (DLQ)**:
   - Configure `@RetryableTopic` or custom non-blocking backoff topics with exponential delay (`1s -> 5s -> 30s`).
   - Route poison-pill messages that fail schema validation or exhaust retries to `<topic>.dlq` with original exception stacktrace and OpenTelemetry `traceparent` header preserved.
