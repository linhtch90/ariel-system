# 02 — Business Domain, Financial Invariants & Real-World Scenarios

To maintain or extend **Project Ariel** effectively, developers need to understand **why** the business rules exist—not just how the Java code compiles. This guide explains the business domain, key terminology, strict financial invariants, and five real-world user journeys traced through the microservices.

---

## 1. The Business Model: Who Uses Project Ariel?

**Project Ariel** operates as a **multi-tenant B2B2C Real-Time Transaction Processing, Fraud Prevention, and Financial Telemetry Platform**.

Think of platforms like **Stripe**, **Adyen**, **PayPal Braintree**, or the internal payment & risk backbone of **Grab / Uber / Shopee**:
1. **Tenants (Merchants / Financial Partners)**: Businesses (identified by `X-Tenant-Id`, e.g., `tenant-acme`) that integrate with Ariel's API to process payments, wallet transfers, or high-value orders for their end customers.
2. **End Customers (`accountId`) & Counterparties (`counterpartyId`)**: The payer and payee involved in a financial movement.
3. **Risk & Compliance Officers**: Internal and tenant teams who configure fraud thresholds, monitor live P99 risk scores, and review flagged high-risk transactions.
4. **Merchant Finance & Analytics Teams**: Business users who view real-time volume dashboards and historical audit logs via the Query API.

---

## 2. Domain Glossary (Ubiquitous Language)

Every class and record in `libs:ariel-common-domain` maps directly to a core business concept:

| Business Term | Code Representation | Business Meaning |
| :--- | :--- | :--- |
| **Tenant** | `tenantId` (`X-Tenant-Id`) | A partner organization using the platform. Rate limits, cache keys, risk rules, and analytics are strictly isolated per tenant. |
| **Idempotency Key** | `idempotencyKey` (`Idempotency-Key`) | A unique client-generated UUID per checkout/payment attempt. Guarantees that if a mobile app retries a timed-out request 5 times, the customer is only charged **once**. |
| **Correlation ID** | `correlationId` (`X-Correlation-Id`) | A globally unique trace identifier attached to a request at the Edge Gateway and propagated across all HTTP calls, Virtual Threads, database rows, and Kafka events. |
| **Transaction Command** | [`TransactionCommand`](../../libs/ariel-common-domain/src/main/java/io/ariel/common/domain/TransactionCommand.java) | An inbound request to move funds (`amount`, `currency`) between `accountId` and `counterpartyId` in a specific `region` with a `deviceFingerprint`. |
| **Velocity** | `evaluateAccountVelocity` | How much money a single `accountId` has transacted within a rolling time window. A sudden spike in velocity is the #1 indicator of a compromised account or stolen card. |
| **Risk Decision** | [`RiskDecision`](../../libs/ariel-common-domain/src/main/java/io/ariel/common/domain/RiskDecision.java) (`sealed interface`) | The binding judgment of the Risk Engine. Must be one of three mutually exclusive outcomes: **`Approved`** (proceed immediately), **`ManualReviewRequired`** (hold for compliance), or **`Rejected`** (block immediately). |
| **Ledger Entry** | [`LedgerEntryEntity`](../../services/ariel-ledger-service/src/main/java/io/ariel/ledger/domain/LedgerEntryEntity.java) | An **immutable** accounting record written once a transaction is approved. Ledger entries are never updated or deleted—corrections are always new compensating entries. |
| **Transactional Outbox** | [`OutboxEventEntity`](../../libs/ariel-outbox-starter/src/main/java/io/ariel/outbox/OutboxEventEntity.java) | A staging table in PostgreSQL written in the *same* database transaction as the Ledger Entry so we never update a balance without emitting its Kafka event. |
| **Poison Pill & DLQ** | `ariel.dlq` (Dead Letter Queue) | A corrupted or unprocessable Kafka message that is quarantined into a separate topic (`ariel.dlq`) so it does not block millions of valid events behind it. |

---

## 3. The 4 Non-Negotiable Business Invariants

When modifying code in Project Ariel, you must **never** violate these four business invariants:

> [!IMPORTANT]
> **Invariant 1: Zero Double-Charging (Strict Idempotency)**
> If a client sends two or more requests with the same `(tenantId, idempotencyKey)`, the system MUST execute the ledger mutation at most once and return the original `TransactionReceipt` with `idempotentReplay = true`.

> [!IMPORTANT]
> **Invariant 2: Risk Before Mutation**
> No transaction may touch `ariel-ledger-service` unless `ariel-risk-engine` has explicitly returned `RiskDecision.Approved`. Both `Rejected` and `ManualReviewRequired` transactions must never mutate `account_balances`.

> [!IMPORTANT]
> **Invariant 3: Atomic Ledger + Event Emission (Zero Dual-Write)**
> Every committed `LedgerEntryEntity` in PostgreSQL MUST have a corresponding `OutboxEventEntity` committed in the **exact same ACID database transaction** (`Propagation.MANDATORY`). Never call `kafkaTemplate.send(...)` directly inside a `@Transactional` business method.

> [!IMPORTANT]
> **Invariant 4: Read Queries Must Never Degrade Live Checkouts**
> Merchant dashboard queries (`GET /api/v1/analytics/**`) must be served by `ariel-query-service` reading from ClickHouse, ScyllaDB, and Redis—never by running heavy `SUM()` or `GROUP BY` queries against the live PostgreSQL `ledger_entries` table.

---

## 4. Five Real-World Business Scenarios Traced Through the Code

### Scenario 1: Happy-Path Mobile Checkout (`<15ms` Fast Lane)
* **Business Story**: Alice buys a $120.50 pair of shoes on Merchant Acme's mobile app using her verified iPhone (`deviceFingerprint = "device-ok"`).
* **Step-by-Step Code Flow**:
  1. **Edge Gateway** ([`EdgeGatewayController`](../../services/ariel-edge-gateway/src/main/java/io/ariel/gateway/web/EdgeGatewayController.java)): Verifies `tenant-acme` has remaining rate-limit tokens in Redis (`RedisTokenBucketRateLimiter`) and forwards the request to `ariel-ingestion-service`.
  2. **Ingestion Service** ([`IngestionOrchestrationUseCase`](../../services/ariel-ingestion-service/src/main/java/io/ariel/ingestion/usecase/IngestionOrchestrationUseCase.java)): Acquires an idempotency lock (`IdempotencyStatus.Acquired`) for `Idempotency-Key` and calls `RiskEngineClient.evaluate(...)`.
  3. **Risk Engine** ([`ParallelRiskEvaluationService`](../../services/ariel-risk-engine/src/main/java/io/ariel/risk/service/ParallelRiskEvaluationService.java)): Spawns 3 parallel Virtual Threads via `StructuredTaskScope`:
     - Velocity check sees normal spending $\rightarrow$ low score.
     - Tenant rule check in L1 Caffeine cache sees `STANDARD_TIER` and `US-EAST` $\rightarrow$ `0.05`.
     - Anomaly heuristic sees valid `deviceFingerprint` and amount `< $25,000` $\rightarrow$ `0.02`.
     - Combined risk score is `~0.03` (`< 0.55`), returning `RiskDecision.Approved(0.03, "FAST_LANE", ...)`.
  4. **Ledger Service** ([`LedgerCommandService`](../../services/ariel-ledger-service/src/main/java/io/ariel/ledger/service/LedgerCommandService.java)): In one PostgreSQL ACID transaction, updates Alice's balance, inserts the `LedgerEntryEntity`, and stages a `ValidatedTransactionEvent` in `outbox_events`.
  5. **Response**: Alice sees "Payment Approved" (`HTTP 202 Accepted`) in ~8 milliseconds.

---

### Scenario 2: Network Blip in a Subway Tunnel (Idempotent Retry)
* **Business Story**: Alice's phone enters a subway tunnel right after tapping "Pay". The payment succeeded on Ariel's servers, but Alice's phone lost cellular signal before receiving the HTTP response. Three seconds later, the mobile app automatically retries the `POST /api/v1/transactions` request with the **same `Idempotency-Key`**.
* **Step-by-Step Code Flow**:
  1. `IngestionOrchestrationUseCase` calls `idempotencyService.tryAcquire("tenant-acme", "idem-key-123", ...)`.
  2. `RedisIdempotencyService` finds the completed JSON receipt in **L1 Caffeine / L2 Redis** and returns `IdempotencyStatus.Completed(cachedJson)`.
  3. The `switch (idempStatus)` branch immediately deserializes the original receipt, sets `idempotentReplay = true`, and returns `HTTP 200 OK` in **under 1 millisecond**—without calling the Risk Engine or charging Alice's account a second time.

---

### Scenario 3: High-Value Corporate Wire ($75,000) Held for Compliance Review
* **Business Story**: A corporate buyer initiates a `$75,000.00` payout. It isn't outright fraud, but company policy requires human compliance sign-off for transactions above `$50,000.00`.
* **Step-by-Step Code Flow**:
  1. `ParallelRiskEvaluationService` evaluates the request and detects `request.amount().compareTo(REVIEW_THRESHOLD) > 0` (`$75,000 > $50,000`).
  2. It returns `new RiskDecision.ManualReviewRequired(score, "TIER_2_COMPLIANCE_QUEUE", ...)`.
  3. In `IngestionOrchestrationUseCase`, the exhaustive `switch (decision)` matches `case RiskDecision.ManualReviewRequired(...)`:
     - It **skips** `ledgerServiceClient.recordTransaction(...)` so no money moves yet.
     - It returns a receipt with status `"QUEUED_FOR_MANUAL_REVIEW:TIER_2_COMPLIANCE_QUEUE"` and caches that receipt under the `Idempotency-Key`.

---

### Scenario 4: Stolen Card Velocity Attack / Over-Limit Fraud (`HTTP 422`)
* **Business Story**: An attacker attempts a `$2,500,000.00` transaction (or rapidly drains an account past `$500,000` velocity from an unknown device in a sanctioned region).
* **Step-by-Step Code Flow**:
  1. `ParallelRiskEvaluationService` computes a combined risk score $\ge 0.85$ (or exceeds `HARD_LIMIT`) and returns `new RiskDecision.Rejected(score, "HIGH_RISK_THRESHOLD_EXCEEDED", ...)`.
  2. `IngestionOrchestrationUseCase` matches `case RiskDecision.Rejected(...)` and throws `ArielBusinessException(HttpStatus.UNPROCESSABLE_ENTITY, "RISK_REJECTED_HIGH_RISK_THRESHOLD_EXCEEDED", ...)`.
  3. `GlobalProblemDetailExceptionHandler` translates this into an **RFC 9457 `ProblemDetail`** JSON response (`HTTP 422 Unprocessable Entity`) containing the `correlationId` and `tenantId`.

---

### Scenario 5: Real-Time Merchant Analytics & Stream Processing
* **Business Story**: During a flash sale, 10,000 transactions/sec are approved. Merchant Acme's CFO refreshes their live executive dashboard to see total revenue and high-risk transaction counts.
* **Step-by-Step Code Flow**:
  1. `OutboxRelayWorker` in `ariel-ledger-service` polls unpublished rows from `outbox_events` and publishes them to Apache Kafka topic `ariel.transactions.validated`.
  2. `ValidatedTransactionStreamProcessor` in `ariel-stream-processor` consumes the batch, uses **Java 25 `Stream.gather(Gatherers.windowFixed(100))`** to slice the stream into 100-event micro-batches, and uses `StructuredTaskScope` to write in parallel to **ScyllaDB** (time-series audit trail), **ClickHouse** (OLAP aggregations), and **Redis** (invalidating stale cache entries).
  3. When the CFO calls `GET /api/v1/analytics/tenants/tenant-acme/summary`, `AnalyticsQueryService` in `ariel-query-service` loads the fresh aggregation from ClickHouse/projection store and caches it in `TwoTierNearCache` (`L1 Caffeine + L2 Redis`) so the next 1,000 dashboard refreshes return in `<100µs`.
