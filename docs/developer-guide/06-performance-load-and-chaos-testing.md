# 06 — Performance, High-Load, System Limits & Chaos Testing Guide

A cloud-native platform claiming **`<15ms` P99 latency**, **`99.99%+` availability**, and **elastic horizontal scaling (`3 -> 100+` pods)** must prove these claims under automated stress, limit saturation, and fault injection.

Project Ariel includes two complementary testing suites:
1. **Automated In-Process JVM Load, Limit & Chaos Suite (`tests:ariel-load-and-chaos-tests`)** — Runs directly in JUnit 5 on Java 25 Virtual Threads during `./gradlew check`.
2. **External Cluster Load & Autoscaling Suite (`tests/k6/ariel-scale-and-chaos.js` & `tests/load-test.sh`)** — Drives HTTP traffic against Docker Compose or Kubernetes to trigger **HPA** and **KEDA** scale-up and scale-down.

---

## 1. Automated Load, Limit & Chaos Suite (`tests:ariel-load-and-chaos-tests`)

Run the entire suite with a single command:

```bash
./gradlew :tests:ariel-load-and-chaos-tests:test --info
```

### 1.1 High-Load & Latency Benchmark ([`HighLoadLatencyBenchmarkTest.java`](../../tests/ariel-load-and-chaos-tests/src/test/java/io/ariel/chaos/HighLoadLatencyBenchmarkTest.java))
- **What It Tests**: Spawns **10,000 concurrent requests across Java 25 Virtual Threads** (`Executors.newVirtualThreadPerTaskExecutor()`) through the complete fast-path pipeline:
  `RedisTokenBucketRateLimiter` $\rightarrow$ `IngestionOrchestrationUseCase` $\rightarrow$ `RedisIdempotencyService` $\rightarrow$ `ParallelRiskEvaluationService` (`StructuredTaskScope` 3-way parallel check) $\rightarrow$ `LedgerServiceClient`.
- **What It Measures**:
  - Total wall-clock time and sustained **Requests Per Second (RPS)**.
  - **P50, P95, and P99 latencies** across all 10,000 concurrent transactions.

---

### 1.2 Reaching System Limits & Backpressure ([`SystemLimitsAndBackpressureTest.java`](../../tests/ariel-load-and-chaos-tests/src/test/java/io/ariel/chaos/SystemLimitsAndBackpressureTest.java))
- **Limit Test 1 — Noisy-Neighbor Multi-Tenant Rate Limiting**:
  - Fires `250` burst requests from `tenant-noisy-attacker` against a `100` token-bucket ceiling while `tenant-vip-enterprise` sends `100` normal requests.
  - **Proves**: Exactly `100` attacker requests succeed, `150` are immediately shed with `HTTP 429 TENANT_RATE_LIMIT_EXCEEDED`, and `tenant-vip-enterprise` experiences **0% packet loss** (strict multi-tenant isolation).
- **Limit Test 2 — `SemaphoreBulkhead` Concurrency Saturation**:
  - Simulates a downstream slowdown where `10` active calls hold all `10` concurrency permits of the Resilience4j `SemaphoreBulkhead`, while `40` additional virtual thread requests arrive simultaneously.
  - **Proves**: All `40` excess requests are rejected immediately (`BulkheadFullException` $\rightarrow$ `HTTP 429`) in `<0.1ms` rather than queuing indefinitely and exhausting JVM heap or socket pools.

---

### 1.3 Chaos Engineering & Fault Injection ([`ChaosFaultInjectionResilienceTest.java`](../../tests/ariel-load-and-chaos-tests/src/test/java/io/ariel/chaos/ChaosFaultInjectionResilienceTest.java))
- **Chaos Experiment 1 — Downstream Risk Engine Outage & CircuitBreaker Trip**:
  - Injects a 100% connection failure into `RiskEngineClient`.
  - **Proves**: After filling the sliding window, the Resilience4j `CircuitBreaker` transitions from `CLOSED` to **`OPEN`**, failing fast with `CallNotPermittedException` (`HTTP 503 ProblemDetail`) without making network calls to the dead dependency.
- **Chaos Experiment 2 — 400ms Dependency Hang vs. 25ms `StructuredTaskScope` Deadline**:
  - Injects a `400ms` latency spike into a risk evaluation subtask while `deadlineMillis = 25ms`.
  - **Proves**: Java 25 `StructuredTaskScope` automatically interrupts the hung virtual thread at `25ms` and returns a safe `RiskDecision.ManualReviewRequired("TIMEOUT_FALLBACK_QUEUE")` fallback instead of hanging the checkout request.
- **Chaos Experiment 3 — Total Redis L2 Cluster Blackout**:
  - Injects `RedisConnectionFailureException` into every `StringRedisTemplate` operation mid-flight.
  - **Proves**: `TwoTierNearCache` and `RedisIdempotencyService` degrade gracefully to pod-local **Caffeine L1** and local locks with **zero unhandled exceptions**.
- **Chaos Experiment 4 — 5,000-Event Kafka Stream Flood + Poison-Pill Injection**:
  - Floods `ValidatedTransactionStreamProcessor` with `5,000` valid events mixed with `25` corrupted JSON poison pills.
  - **Proves**: Java 25 `Stream.gather(Gatherers.windowFixed(100))` processes all `5,000` valid events into ScyllaDB and ClickHouse in parallel while quarantining all `25` corrupted payloads into `ariel.dlq`.

---

## 2. Testing Horizontal Autoscaling (Scale-Up & Scale-Down)

To test **Kubernetes HPA (`3 -> 100+ pods`)** and **KEDA (`3 -> 64 pods`)** under live traffic spikes:

1. **Deploy the manifests** in [`deploy/k8s/`](../../deploy/k8s/):
   ```bash
   kubectl apply -f deploy/k8s/ariel-deployments-and-hpa.yaml
   kubectl apply -f deploy/k8s/ariel-keda-kafka-scaler.yaml
   ```
2. **Run the 4-stage Grafana k6 Autoscaling Test** ([`tests/k6/ariel-scale-and-chaos.js`](../../tests/k6/ariel-scale-and-chaos.js)):
   ```bash
   k6 run tests/k6/ariel-scale-and-chaos.js
   ```
   - **Stage 1 (30s Warmup, `0 -> 100 VUs`)**: Verifies baseline `<15ms P99` latency at `minReplicas: 3`.
   - **Stage 2 (1m Spike, `100 -> 1,000 VUs`)**: Triggers HPA rapid scale-up policy (`100%` pod increase every `15s` with `0s` stabilization window) and KEDA Kafka lag scale-up (`lagThreshold: 5000`).
   - **Stage 3 (2m Sustained Peak, `1,000 VUs`)**: Verifies Generational ZGC pause times (`<0.5ms`), L1/L2 cache hit ratio, and `10%` idempotent retry replay under peak load.
   - **Stage 4 (30s Cool-off, `1,000 -> 0 VUs`)**: Verifies HPA `scaleDown.stabilizationWindowSeconds: 300` and KEDA `cooldownPeriod: 180` prevent premature pod thrashing as traffic subsides.
