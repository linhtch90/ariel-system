---
name: resilience-and-scaling-audit
description: >-
  Use this skill when auditing or configuring fault tolerance (Resilience4j),
  horizontal autoscaling (Kubernetes HPA / KEDA), and high-load performance tuning.
---

# Skill: Cloud-Native Resilience & Horizontal Scaling Audit

Use this checklist and configuration guide to verify that any Ariel microservice is production-ready for extreme traffic spikes and partial infrastructure failures.

## Step 1: Verify Resilience4j Protection Order
Ensure external calls apply Resilience4j decorators in the standard outer-to-inner aspect order:
`Retry ( Bulkhead ( TimeLimiter ( RateLimiter ( CircuitBreaker ( Function ) ) ) ) )`

1. **Circuit Breaker**:
   - `slidingWindowType: COUNT_BASED` (or `TIME_BASED`)
   - `slidingWindowSize: 100`
   - `failureRateThreshold: 50`
   - `slowCallRateThreshold: 80`
   - `slowCallDurationThreshold: 200ms`
   - `waitDurationInOpenState: 10s`
2. **Bulkhead**:
   - Use **SemaphoreBulkhead** (not FixedThreadPoolBulkhead) when running on **Java 25 Virtual Threads** so virtual threads are throttled by concurrency permits without creating redundant thread pools.
3. **Retry with Jitter**:
   - Only retry idempotent operations and transient network/timeout errors.
   - Use `enableRandomizedWait: true` (`randomizedWaitFactor: 0.5`) to prevent retry storms across pods.

## Step 2: Verify Horizontal Pod Autoscaling (HPA & KEDA)
1. **Stateless API Services (`ariel-ingestion-service`, `ariel-risk-engine`, `ariel-query-service`)**:
   - Ensure zero local state affinity.
   - Configure Kubernetes `HorizontalPodAutoscaler` scaling on CPU utilization (70%) and custom Prometheus metric `http_server_requests_seconds_count`.
2. **Event Consumer Services (`ariel-stream-processor`)**:
   - Configure a KEDA `ScaledObject` targeting the Apache Kafka consumer group lag (`lagThreshold: "5000"` per partition), scaling from `minReplicaCount: 3` up to `maxReplicaCount: <topic_partition_count>`.

## Step 3: Verify JVM & Connection Pool Sizing for Scale
- **JVM Memory & GC**: `-XX:+UseZGC -XX:+ZGenerational -XX:MaxRAMPercentage=75.0`
- **Database Pool (HikariCP)**: Keep pool size bounded (`maximum-pool-size: 20-30` per pod) even when thousands of Virtual Threads are active, preventing PostgreSQL connection exhaustion during pod scale-out (or route through PgBouncer).
