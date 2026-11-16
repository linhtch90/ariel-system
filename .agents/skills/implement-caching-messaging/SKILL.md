---
name: implement-caching-messaging
description: >-
  Use this skill when implementing or extending multi-tier caching (L1 Caffeine + L2 Redis)
  or durable event streaming (Apache Kafka + Transactional Outbox) in Ariel services.
---

# Skill: Implement Multi-Tier Caching & Durable Event Messaging

Use this runbook to work with the caching and messaging infrastructure in the Ariel System.

## Part A: Two-Tier Near-Cache (`libs:ariel-cache-starter`)

### Reference Implementations
- [`TwoTierNearCache.java`](../../../libs/ariel-cache-starter/src/main/java/io/ariel/cache/TwoTierNearCache.java)
- [`RedisIdempotencyService.java`](../../../libs/ariel-cache-starter/src/main/java/io/ariel/cache/RedisIdempotencyService.java)
- [`ArielCacheAutoConfiguration.java`](../../../libs/ariel-cache-starter/src/main/java/io/ariel/cache/ArielCacheAutoConfiguration.java)

### How to Use in a Microservice
1. Add `implementation project(':libs:ariel-cache-starter')` to `build.gradle`.
2. Inject `TwoTierNearCache` into your service constructor:
   - **Read-Through with Single-Flight Stampede Protection**:
     ```java
     String json = nearCache.getOrLoad("ariel:domain:key:" + id, k -> loadFromDatabase(id));
     ```
   - **Cluster-Wide Invalidation (L1 Caffeine + L2 Redis + Pub/Sub `ariel:cache:invalidation`)**:
     ```java
     nearCache.invalidateClusterWide("ariel:domain:key:" + id);
     ```
3. **Idempotency Barrier**:
   - Inject `RedisIdempotencyService` and call `tryAcquire(tenantId, idempotencyKey, Duration.ofSeconds(60))`.
   - Handle the sealed `IdempotencyStatus` (`Acquired`, `Completed`, `ConcurrentInProgress`) via an exhaustive Java 25 `switch`.
   - Always call `idempotencyService.releaseOnFailure(lockKey)` in a `catch` block if downstream execution fails so client retries are not blocked by a stale `__IN_PROGRESS__` lock.

## Part B: Durable Kafka Messaging & Transactional Outbox (`libs:ariel-outbox-starter`)

### Reference Implementations
- [`TransactionalOutboxPublisher.java`](../../../libs/ariel-outbox-starter/src/main/java/io/ariel/outbox/TransactionalOutboxPublisher.java)
- [`OutboxRelayWorker.java`](../../../libs/ariel-outbox-starter/src/main/java/io/ariel/outbox/OutboxRelayWorker.java)
- [`ValidatedTransactionStreamProcessor.java`](../../../services/ariel-stream-processor/src/main/java/io/ariel/stream/processor/ValidatedTransactionStreamProcessor.java)

### Rules & Best Practices
1. **Zero Dual-Write Guarantee (`Propagation.MANDATORY`)**:
   - Never call `kafkaTemplate.send(...)` directly inside a `@Transactional` database method.
   - Call `outboxPublisher.stageEvent(aggregateType, aggregateId, topic, eventRecord)` inside your active `@Transactional` method so the `OutboxEventEntity` commits atomically with your domain entity.
2. **Stream Consumer Micro-Batching & DLQ Isolation**:
   - Partition incoming stream batches using Java 25 `Stream.gather(Gatherers.windowFixed(100))`.
   - Fan out sink writes in parallel using `StructuredTaskScope.open(Joiner.awaitAllSuccessfulOrThrow(), ...)`.
   - Catch deserialization/validation errors per message and route poison pills to `ariel.dlq` so valid messages in the batch are never blocked.
