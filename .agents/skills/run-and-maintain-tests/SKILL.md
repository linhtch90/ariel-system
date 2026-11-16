---
name: run-and-maintain-tests
description: >-
  Use this skill when writing, running, debugging, or extending unit tests,
  Spring Boot 4.1.2 MockMvc/JPA integration tests, architectural governance tests,
  or high-load and chaos engineering tests in the Ariel monorepo.
---

# Skill: Write, Run & Maintain Tests in Project Ariel

Use this runbook whenever adding or troubleshooting tests across `libs/*`, `services/*`, or `tests:ariel-load-and-chaos-tests`.

## 1. Key Commands
- Run all unit, integration, architecture, load, and chaos tests:
  ```bash
  ./gradlew check
  ```
- Force re-execution of all tests (e.g. for latency benchmarking):
  ```bash
  ./gradlew check --rerun-tasks
  ```
- Run a single module or single test class:
  ```bash
  ./gradlew :services:ariel-ledger-service:test --tests "*LedgerCommandServiceIntegrationTest"
  ```

## 2. Verified Spring Boot 4.1.2 & Java 25 Testing Idioms

1. **Spring Boot 4.1.2 Test Imports**:
   - `@AutoConfigureMockMvc`: `import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;` (requires `testImplementation libs.spring.boot.starter.webmvc.test` in `build.gradle`).
   - `@MockitoBean`: `import org.springframework.test.context.bean.override.mockito.MockitoBean;` (replaces removed `@MockBean`).
   - Jackson 3 `JsonMapper`: `import tools.jackson.databind.json.JsonMapper;`.
2. **Preventing Test Hangs on Offline Local Ports**:
   - Always add `"management.otlp.metrics.export.enabled=false"` to `@SpringBootTest(properties = ...)` so context shutdown does not log `ConnectException` against `localhost:4318`.
   - In `@SpringBootTest` slice tests where Docker Kafka (`localhost:9092`) or Redis (`localhost:6379`) is not running, declare `@MockitoBean private KafkaTemplate<String, String> kafkaTemplate;` or `@MockitoBean private StringRedisTemplate redisTemplate;` so Lettuce/Kafka clients do not block or trip strict `50ms` `StructuredTaskScope` deadlines.
   - For services with `@Scheduled` outbox workers (`ariel-ledger-service`), set `"ariel.outbox.initial-delay-ms=600000"` and `"ariel.outbox.poll-interval-ms=600000"` in `@SpringBootTest` properties so you can deterministically assert unpublished rows before calling `outboxRelayWorker.relayPendingBatch()`.
3. **Testing `ScopedValue<RequestContext>`**:
   - Wrap use-case calls in `RequestContextHolder.callWith(ctx, () -> ...)` or `RequestContextHolder.runWith(ctx, () -> ...)`.
4. **Java 25 Classfile Version (`69.0`) & Architectural Governance**:
   - Because Java 25 emits classfile major version `69` (`65535` with `--enable-preview`), avoid old shaded ASM scanners. Add new architectural assertions to [`ArielArchitectureGovernanceTest.java`](../../../tests/ariel-load-and-chaos-tests/src/test/java/io/ariel/chaos/ArielArchitectureGovernanceTest.java) using Java 25 reflection (`Class.isRecord()`, `Class.isSealed()`) or `java.lang.classfile.ClassFile`.
