# 04 — Extending, Maintaining & Troubleshooting Playbook

This playbook provides step-by-step recipes for common engineering tasks you will perform when adding features, evolving database schemas, or operating **Project Ariel** in production.

---

## Recipe 1: How to Add a New Fraud/Risk Check to `ariel-risk-engine`

Suppose product management asks you to add a 4th real-time risk check (e.g., **Counterparty Blacklist Check**) that runs in parallel alongside the existing Velocity, Tenant Rule, and Anomaly checks.

1. **Open** [`ParallelRiskEvaluationService.java`](../../services/ariel-risk-engine/src/main/java/io/ariel/risk/service/ParallelRiskEvaluationService.java).
2. **Write the check method** (using `TwoTierNearCache` so hot lookups take `<100µs`):
   ```java
   private double evaluateCounterpartyReputation(RiskEvaluationRequest request) {
       String key = "ariel:reputation:counterparty:" + request.counterpartyId();
       String status = nearCache.getOrLoad(key, _ -> "TRUSTED");
       return "BLACKLISTED".equals(status) ? 1.0 : 0.01;
   }
   ```
3. **Fork the new subtask inside `StructuredTaskScope`**:
   ```java
   var velocityTask     = scope.fork(() -> evaluateAccountVelocity(request));
   var ruleTask         = scope.fork(() -> evaluateTenantRules(request));
   var anomalyTask      = scope.fork(() -> evaluateAnomalyHeuristics(request));
   var counterpartyTask = scope.fork(() -> evaluateCounterpartyReputation(request));

   scope.join();
   ```
4. **Add a unit test** in [`ParallelRiskEvaluationServiceTest.java`](../../services/ariel-risk-engine/src/test/java/io/ariel/risk/service/ParallelRiskEvaluationServiceTest.java) and run:
   ```bash
   ./gradlew :services:ariel-risk-engine:test
   ```

---

## Recipe 2: How to Add a New Endpoint & Declarative `@HttpExchange` Client

Suppose you want to expose `GET /api/v1/ledger/accounts/{accountId}/balance` on `ariel-ledger-service` and route to it from `ariel-query-service` or `ariel-edge-gateway`.

1. **Define the DTO** in `libs/ariel-common-domain/src/main/java/io/ariel/common/domain/` as a Java 25 `record`:
   ```java
   public record AccountBalanceDto(String accountId, String tenantId, BigDecimal balance, String currency, Instant updatedAt) {}
   ```
2. **Add the Controller Endpoint** in `ariel-ledger-service` ([`LedgerController.java`](../../services/ariel-ledger-service/src/main/java/io/ariel/ledger/web/LedgerController.java)):
   ```java
   @GetMapping("/accounts/{accountId}/balance")
   public AccountBalanceDto getBalance(@PathVariable String accountId) { ... }
   ```
3. **Declare the `@HttpExchange` Method** on the caller's client interface (e.g., [`DownstreamClientsConfig.java`](../../services/ariel-edge-gateway/src/main/java/io/ariel/gateway/client/DownstreamClientsConfig.java)):
   ```java
   @GetExchange("/api/v1/ledger/accounts/{accountId}/balance")
   AccountBalanceDto getAccountBalance(@PathVariable("accountId") String accountId);
   ```
   > **Automatic Context Propagation**: Because both services include `libs:ariel-observability-starter`, `X-Correlation-Id`, `X-Tenant-Id`, and `Idempotency-Key` headers are propagated automatically!

---

## Recipe 3: How to Evolve the Database Schema Safely (Zero-Downtime Flyway Migrations)

In a `99.99%+` High-Availability system where multiple pods run concurrently during a Kubernetes rolling deployment, **never modify an existing Flyway SQL file** (`V1__init_ledger_and_outbox.sql`) and **never rename or drop a column in a single deployment**.

Use the **Expand-and-Contract Pattern**:
1. **Create a new versioned migration** in `services/ariel-ledger-service/src/main/resources/db/migration/V2__add_settlement_reference.sql`:
   ```sql
   -- Step 1 (Expand): Add nullable column or column with default so both old (v1) and new (v2) pods work simultaneously
   ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS settlement_ref VARCHAR(64);
   ```
2. **Update the JPA Entity** ([`LedgerEntryEntity.java`](../../services/ariel-ledger-service/src/main/java/io/ariel/ledger/domain/LedgerEntryEntity.java)) to map the new column.
3. **Verify with Integration Tests**:
   ```bash
   ./gradlew :services:ariel-ledger-service:test
   ```

---

## Recipe 4: How to Scaffold a Brand-New Microservice in the Monorepo

If you need to add a 7th microservice (for example, `services/ariel-notification-service`):
1. Add `include 'services:ariel-notification-service'` to [`settings.gradle`](../../settings.gradle).
2. Create `services/ariel-notification-service/build.gradle` with:
   ```groovy
   plugins {
       id 'ariel.spring-boot-service'
   }

   dependencies {
       implementation project(':libs:ariel-common-domain')
       implementation project(':libs:ariel-observability-starter')
   }
   ```
3. Enable Virtual Threads and graceful shutdown in `src/main/resources/application.yml` (see [`.agents/skills/scaffold-spring-service/SKILL.md`](../../.agents/skills/scaffold-spring-service/SKILL.md)).

---

## Recipe 5: Production Troubleshooting & SRE Runbook

### Symptom A: Client Receives `HTTP 429 Too Many Requests` (`TENANT_RATE_LIMIT_EXCEEDED`)
- **Cause**: The tenant exceeded their per-second burst allocation in [`RedisTokenBucketRateLimiter`](../../services/ariel-edge-gateway/src/main/java/io/ariel/gateway/ratelimit/RedisTokenBucketRateLimiter.java).
- **How to Diagnose & Resolve**:
  - Inspect the `correlationId` and `tenantId` in the RFC 9457 `ProblemDetail` JSON body.
  - Check `X-RateLimit-Remaining` response headers or adjust `ariel.gateway.rate-limit.burst-capacity` in `ariel-edge-gateway/src/main/resources/application.yml`.

### Symptom B: Client Receives `HTTP 409 Conflict` (`DUPLICATE_IN_FLIGHT_REQUEST`)
- **Cause**: The client sent two concurrent requests with the **exact same `Idempotency-Key`** while the first request is still executing (`__IN_PROGRESS__` lock held in `RedisIdempotencyService`).
- **Resolution**: Once the first request finishes (typically `<15ms`), any subsequent retry with that `Idempotency-Key` will return `HTTP 200 OK` with the cached `TransactionReceipt` (`idempotentReplay: true`).

### Symptom C: Client Receives `HTTP 503 Service Unavailable` (`CIRCUIT_BREAKER_OPEN`)
- **Cause**: A downstream dependency (`ariel-risk-engine`, `ariel-ingestion-service`, or `ariel-query-service`) exceeded the Resilience4j failure rate threshold, tripping the CircuitBreaker OPEN to fail fast and allow recovery.
- **How to Diagnose**: Check `/actuator/health` and `/actuator/prometheus` on the target service.

### Symptom D: Kafka Consumer Lag Spikes or Messages Appear in `ariel.dlq`
- **How Automatic Scaling Responds**: In Kubernetes, **KEDA** ([`ariel-keda-kafka-scaler.yaml`](../../deploy/k8s/ariel-keda-kafka-scaler.yaml)) monitors consumer lag on `ariel.transactions.validated` and automatically scales `ariel-stream-processor` from `3` up to `64` pods when lag exceeds `5,000` messages per partition.
- **How Poison Pills Are Handled**: If a malformed message fails deserialization or validation in [`ValidatedTransactionStreamProcessor`](../../services/ariel-stream-processor/src/main/java/io/ariel/stream/processor/ValidatedTransactionStreamProcessor.java), it is isolated into the `ariel.dlq` topic without blocking valid messages in the batch.

---

## Recipe 6: Updating Architecture Diagrams

Whenever you change service interactions, caching layers, or scaling topologies:
1. Edit the corresponding `.puml` file in [`docs/architecture/`](../architecture/).
2. Re-render both SVG and PNG files:
   ```bash
   plantuml -DPLANTUML_LIMIT_SIZE=8192 -tsvg docs/architecture/*.puml
   plantuml -DPLANTUML_LIMIT_SIZE=8192 -tpng docs/architecture/*.puml
   ```
