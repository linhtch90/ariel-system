# Phase 2 Prompt — Implement Core Microservices (Fast Path)

```markdown
Act as `java-microservice-engineer` and activate the `scaffold-spring-service` skill.

Task: Implement the synchronous low-latency (<15ms P99) microservices in `services/`:
1. `services/ariel-edge-gateway`:
   - Spring Cloud Gateway with Redis rate limiting, JWT validation, and Resilience4j circuit breaking.
2. `services/ariel-ingestion-service`:
   - High-throughput REST & gRPC ingestion endpoint running on Java 25 Virtual Threads.
   - Redis + Caffeine idempotency barrier (`Idempotency-Key` header).
   - Orchestrates synchronous call to `ariel-risk-engine` and command persistence to `ariel-ledger-service`.
3. `services/ariel-risk-engine`:
   - Uses Java 25 `StructuredTaskScope.ShutdownOnFailure` to run parallel velocity, geo-ip, and rule checks within a strict 5ms deadline.
4. `services/ariel-ledger-service`:
   - Persists transactions into partitioned PostgreSQL 17 tables and writes `OutboxEvent` records atomically in the same ACID transaction.
5. Include Testcontainers integration tests verifying idempotency, structured concurrency cancellation, and transactional outbox persistence.
```
