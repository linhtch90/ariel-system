---
name: resilience-and-scaling-audit
description: >-
  Use this skill when auditing or configuring fault tolerance (Resilience4j),
  horizontal autoscaling (Kubernetes HPA / KEDA), and running high-load or chaos benchmarks.
---

# Skill: Cloud-Native Resilience, Horizontal Scaling & Chaos Audit

Use this checklist and reference guide to verify fault tolerance, capacity limits, and autoscaling behavior across Ariel microservices.

## 1. Reference Implementations in the Repository
- **Perimeter Rate Limiter**: [`RedisTokenBucketRateLimiter.java`](../../../services/ariel-edge-gateway/src/main/java/io/ariel/gateway/ratelimit/RedisTokenBucketRateLimiter.java)
- **SemaphoreBulkhead & CircuitBreaker**: [`IngestionOrchestrationUseCase.java`](../../../services/ariel-ingestion-service/src/main/java/io/ariel/ingestion/usecase/IngestionOrchestrationUseCase.java)
- **`StructuredTaskScope` Deadline Cancellation**: [`ParallelRiskEvaluationService.java`](../../../services/ariel-risk-engine/src/main/java/io/ariel/risk/service/ParallelRiskEvaluationService.java)
- **RFC 9457 `ProblemDetail` Error Mapping**: [`GlobalProblemDetailExceptionHandler.java`](../../../libs/ariel-observability-starter/src/main/java/io/ariel/observability/GlobalProblemDetailExceptionHandler.java)
- **Kubernetes HPA & KEDA Manifests**:
  - [`deploy/k8s/ariel-deployments-and-hpa.yaml`](../../../deploy/k8s/ariel-deployments-and-hpa.yaml)
  - [`deploy/k8s/ariel-keda-kafka-scaler.yaml`](../../../deploy/k8s/ariel-keda-kafka-scaler.yaml)

## 2. Resilience4j & Virtual Thread Tuning Rules
1. **SemaphoreBulkhead (Never ThreadPoolBulkhead)**:
   - Because services run on **Java 25 Virtual Threads**, always use `SemaphoreBulkhead` (`Bulkhead.of(...)`) to bound downstream concurrency permits (`maxConcurrentCalls: 500`) without creating redundant OS thread pools.
2. **Benchmark Concurrency Alignment**:
   - When writing in-process Virtual Thread load benchmarks ([`HighLoadLatencyBenchmarkTest.java`](../../../tests/ariel-load-and-chaos-tests/src/test/java/io/ariel/chaos/HighLoadLatencyBenchmarkTest.java)), bound concurrent virtual users (`new Semaphore(64)`) below the per-pod `500`-permit `SemaphoreBulkhead` so you measure sustained throughput (`>20,000 RPS`, `<15ms P99`) rather than intentionally tripping the bulkhead.
3. **JVM Runtime Flags**:
   - Use `--enable-preview -XX:+UseZGC` (note: in Java 25, ZGC is generational by default; do not pass the removed `-XX:+ZGenerational` flag).

## 3. Running Automated Load, Limits & Chaos Verification
1. Execute the automated JVM load, limit-saturation, and 4-experiment Chaos suite:
   ```bash
   ./gradlew :tests:ariel-load-and-chaos-tests:test --rerun-tasks
   ```
2. Execute the external 4-stage Grafana k6 HPA/KEDA autoscaling spike test:
   ```bash
   k6 run tests/k6/ariel-scale-and-chaos.js
   ```
