# 03 — Java 25 & Spring Boot 4.1.2 Codebase Tour (Junior-to-Senior Guide)

One of the goals of **Project Ariel** is to serve as a living reference for **modern Java 25** and **Spring Boot v4.1.2** in a cloud-native architecture.

This guide walks through the key technical patterns used in the codebase, explaining **what problem each feature solves**, **how it compares to older Java/Spring approaches**, and **where to find it in the repository**.

---

## 1. Java 25 Language & Concurrency Features in Ariel

### 1.1 Virtual Threads (`spring.threads.virtual.enabled=true`)
* **Where to see it**: Every `application.yml` in `services/*/src/main/resources/application.yml`.
* **For Junior Developers**:
  - In traditional Java, every HTTP request occupied an OS platform thread (~1MB RAM). When that thread waited 10ms for PostgreSQL, Redis, or another microservice to respond, the OS thread sat idle, limiting a server to ~200–500 concurrent requests before thread-pool exhaustion.
  - With **Java 25 Virtual Threads**, blocking I/O calls (`RestClient`, `JdbcTemplate`, `StringRedisTemplate`) automatically **unmount** the lightweight virtual thread from the OS carrier thread, allowing a single pod to handle **tens of thousands of concurrent requests** using simple, readable synchronous code (without complex reactive `Mono`/`Flux` callbacks).
* **For Senior Engineers**:
  - Because Virtual Threads make thread creation virtually free, **never pool Virtual Threads**. Instead, bound downstream concurrency using **Resilience4j `SemaphoreBulkhead`** (see [`IngestionOrchestrationUseCase.java`](../../services/ariel-ingestion-service/src/main/java/io/ariel/ingestion/usecase/IngestionOrchestrationUseCase.java)) and keep **HikariCP `maximum-pool-size`** bounded (`25`) so 10,000 virtual threads don't open 10,000 PostgreSQL connections.

---

### 1.2 Scoped Values (`ScopedValue<RequestContext>`) Instead of `ThreadLocal`
* **Where to see it**:
  - [`RequestContextHolder.java`](../../libs/ariel-common-domain/src/main/java/io/ariel/common/context/RequestContextHolder.java)
  - [`ScopedValueCorrelationFilter.java`](../../libs/ariel-observability-starter/src/main/java/io/ariel/observability/ScopedValueCorrelationFilter.java)
* **Old Way (`ThreadLocal`)**:
  - Mutable: Any method deep in the call stack could accidentally overwrite or clear the `ThreadLocal`.
  - Memory Leaks: If you forgot `threadLocal.remove()` in a `finally` block, context leaked into the next request on pooled threads.
  - Expensive Copying: `InheritableThreadLocal` had to clone maps when spawning child threads.
* **Java 25 Way in Project Ariel (`ScopedValue`)**:
  ```java
  // 1. Declared once in RequestContextHolder.java
  public static final ScopedValue<RequestContext> CONTEXT = ScopedValue.newInstance();

  // 2. Bound for the exact lexical scope of the HTTP request in ScopedValueCorrelationFilter.java
  ScopedValue.where(RequestContextHolder.CONTEXT, context).call(() -> {
      filterChain.doFilter(request, response);
      return null;
  });

  // 3. Read anywhere down the call stack (or inside child StructuredTaskScope virtual threads!)
  var ctx = RequestContextHolder.current();
  ```
  - **Automatic Cleanup**: As soon as `.call(...)` exits, the binding is automatically unbound—memory leaks are impossible.
  - **Zero-Copy Child Inheritance**: Child virtual threads forked inside `StructuredTaskScope` automatically share the parent's `ScopedValue` binding with zero memory copying!

---

### 1.3 Structured Concurrency (`StructuredTaskScope`)
* **Where to see it**:
  - [`ParallelRiskEvaluationService.java`](../../services/ariel-risk-engine/src/main/java/io/ariel/risk/service/ParallelRiskEvaluationService.java)
  - [`ValidatedTransactionStreamProcessor.java`](../../services/ariel-stream-processor/src/main/java/io/ariel/stream/processor/ValidatedTransactionStreamProcessor.java)
* **What Problem It Solves**:
  - In `ariel-risk-engine`, we must run 3 checks before approving a payment: (1) Account Velocity, (2) Tenant Rules, and (3) Anomaly Heuristics.
  - Running them sequentially (`T1 + T2 + T3`) adds up their latencies. Running them with legacy `CompletableFuture.allOf(...)` or `ExecutorService` makes cancellation, deadlines, and thread-leak prevention error-prone.
* **How It Works in Project Ariel**:
  ```java
  try (var scope = StructuredTaskScope.open(
          StructuredTaskScope.Joiner.<Double>awaitAllSuccessfulOrThrow(),
          cf -> cf.withName("risk-evaluation-scope")
                  .withTimeout(Duration.ofMillis(deadlineMillis)))) {

      var velocityTask = scope.fork(() -> evaluateAccountVelocity(request));
      var ruleTask     = scope.fork(() -> evaluateTenantRules(request));
      var anomalyTask  = scope.fork(() -> evaluateAnomalyHeuristics(request));

      scope.join(); // Waits for all 3 virtual threads or cancels all if any fails / times out!

      double combinedScore = (velocityTask.get() * 0.35)
                           + (ruleTask.get() * 0.35)
                           + (anomalyTask.get() * 0.30);
  }
  ```
  - **Short-Circuiting & No Thread Leaks**: If `velocityTask` throws an exception or the `50ms` timeout expires, `StructuredTaskScope` immediately interrupts the remaining subtasks and guarantees all child threads have terminated before exiting the `try-with-resources` block.

---

### 1.4 Sealed Interfaces, Record Patterns & Unnamed Variables (`_`)
* **Where to see it**:
  - [`RiskDecision.java`](../../libs/ariel-common-domain/src/main/java/io/ariel/common/domain/RiskDecision.java)
  - [`IngestionOrchestrationUseCase.java`](../../services/ariel-ingestion-service/src/main/java/io/ariel/ingestion/usecase/IngestionOrchestrationUseCase.java)
* **How It Works**:
  - `RiskDecision` is a `sealed interface` permitting only three `record` subtypes: `Approved`, `Rejected`, and `ManualReviewRequired`.
  - In `IngestionOrchestrationUseCase`, we use an **exhaustive `switch` expression with record deconstruction and unnamed variables (`_`)**:
    ```java
    TransactionReceipt receipt = switch (decision) {
        case RiskDecision.Approved(var score, _, _) -> commitToLedger(command, score);
        case RiskDecision.ManualReviewRequired(var score, var queue, _, _) -> queueForReview(command, score, queue);
        case RiskDecision.Rejected(_, var reasonCode, var description, _) ->
            throw new ArielBusinessException(HttpStatus.UNPROCESSABLE_ENTITY, "RISK_REJECTED_" + reasonCode, description);
    };
    ```
  - **Why Senior Engineers Love This**: There is **no `default:` branch**. If a developer adds a 4th subtype to `RiskDecision` in the future, the Java 25 compiler immediately flags every `switch` in the monorepo that hasn't handled the new case!

---

### 1.5 Stream Gatherers (`Gatherers.windowFixed`)
* **Where to see it**: [`ValidatedTransactionStreamProcessor.java`](../../services/ariel-stream-processor/src/main/java/io/ariel/stream/processor/ValidatedTransactionStreamProcessor.java)
* **How It Works**:
  - Java 24/25 introduced `Stream.gather(...)` (custom intermediate stream operations).
  - Instead of writing manual `for`-loop index slicing to chunk a stream of events into micro-batches for ScyllaDB and ClickHouse bulk inserts, we write:
    ```java
    List<List<ValidatedTransactionEvent>> microBatches = validEvents.stream()
            .gather(Gatherers.windowFixed(microBatchWindowSize))
            .toList();
    ```

---

## 2. Cloud-Native & Spring Boot 4.1.2 Patterns in Ariel

### 2.1 Declarative HTTP Interfaces (`@HttpExchange`)
* **Where to see it**:
  - [`FastPathClientsConfig.java`](../../services/ariel-ingestion-service/src/main/java/io/ariel/ingestion/client/FastPathClientsConfig.java)
  - [`DownstreamClientsConfig.java`](../../services/ariel-edge-gateway/src/main/java/io/ariel/gateway/client/DownstreamClientsConfig.java)
* **Why**: Instead of writing boilerplate `RestTemplate` or `RestClient` URI-building code, we define a pure Java interface annotated with `@HttpExchange` and `@PostExchange`. Spring Boot 4.1.2 generates the proxy client backed by `RestClient` running on Virtual Threads, automatically injecting our `X-Correlation-Id`, `X-Tenant-Id`, and `Idempotency-Key` headers via `ArielObservabilityAutoConfiguration`.

### 2.2 Two-Tier Near-Cache (`L1 Caffeine + L2 Redis`) with Stampede Protection
* **Where to see it**: [`TwoTierNearCache.java`](../../libs/ariel-cache-starter/src/main/java/io/ariel/cache/TwoTierNearCache.java)
* **How It Works**:
  1. **L1 Hit (`<100µs`)**: Checks pod-local Caffeine memory first. Zero network I/O, zero JSON deserialization overhead.
  2. **Single-Flight Stampede Protection**: If a hot key expires while 500 concurrent requests arrive on the same pod, `inFlightLoads.computeIfAbsent(key, ...)` ensures **only 1 Virtual Thread** queries L2 Redis / the database, while the other 499 threads share the same `CompletableFuture` result.
  3. **Cluster-Wide Invalidation**: When data changes, `invalidateClusterWide(key)` deletes the key in Redis and publishes to `ariel:cache:invalidation` so all other pods evict their local L1 copy.

### 2.3 Transactional Outbox Pattern (`Propagation.MANDATORY`)
* **Where to see it**:
  - [`TransactionalOutboxPublisher.java`](../../libs/ariel-outbox-starter/src/main/java/io/ariel/outbox/TransactionalOutboxPublisher.java)
  - [`OutboxRelayWorker.java`](../../libs/ariel-outbox-starter/src/main/java/io/ariel/outbox/OutboxRelayWorker.java)
  - [`LedgerCommandService.java`](../../services/ariel-ledger-service/src/main/java/io/ariel/ledger/service/LedgerCommandService.java)
* **Why**: If a service saves to PostgreSQL and then calls `kafkaTemplate.send()`, and the pod crashes in between, the database and Kafka become permanently inconsistent (the "Dual-Write Bug").
* **How Ariel Solves It**: `TransactionalOutboxPublisher.stageEvent(...)` is annotated with `@Transactional(propagation = Propagation.MANDATORY)`. It writes the event into the PostgreSQL `outbox_events` table as part of the **exact same ACID commit** as `account_balances` and `ledger_entries`. `OutboxRelayWorker` then polls unpublished rows and sends them to Kafka with `acks=all`.
