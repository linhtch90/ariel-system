# Java 25, Spring Boot 4.1.2 & Gradle 9.5 Groovy Engineering Standards (Verified lessons)

This rule file captures the verified, battle-tested conventions and API specifics discovered while building and testing **Project Ariel** on **OpenJDK 25.0.3 LTS**, **Spring Boot v4.1.2 (Spring Framework 7.0 / Jackson 3)**, and **Gradle 9.5.0**.

---

## 1. Gradle 9.5 Groovy Build Conventions (`buildSrc` & `build.gradle`)

- **Convention Plugins First**:
  - Library modules (`libs/*`) MUST apply `id 'ariel.java25-conventions'`.
  - Microservice modules (`services/*`) MUST apply `id 'ariel.spring-boot-service'`.
- **Mandatory Gradle 9.5 + Java 25 Settings** (configured in [`buildSrc/src/main/groovy/ariel.java25-conventions.gradle`](../../buildSrc/src/main/groovy/ariel.java25-conventions.gradle)):
  1. **JUnit Platform Launcher**: Gradle 9.5 requires explicit `testRuntimeOnly 'org.junit.platform:junit-platform-launcher'` alongside `useJUnitPlatform()`.
  2. **Preview Flag (`--enable-preview`)**: Required in Java 25 for `StructuredTaskScope` (JEP 505). Must be passed to `JavaCompile`, `Test`, and `JavaExec`/`bootRun`.
  3. **Generational ZGC (`-XX:+UseZGC`)**: In Java 24/25+, ZGC is generational by default (the legacy `-XX:+ZGenerational` flag was removed in JDK 24/25; pass only `-XX:+UseZGC`).

```groovy
tasks.withType(JavaCompile).configureEach {
    options.encoding = 'UTF-8'
    options.compilerArgs.addAll(['-parameters', '--enable-preview'])
}

tasks.withType(Test).configureEach {
    useJUnitPlatform()
    jvmArgs('--enable-preview', '-XX:+UseZGC')
}
```

---

## 2. Verified Java 25 Concurrency & Language APIs

- **Structured Concurrency (`StructuredTaskScope` — JEP 505 in Java 25)**:
  - **DO NOT** use the removed Java 21 constructor `new StructuredTaskScope.ShutdownOnFailure()`.
  - **ALWAYS** use the Java 25 static factory `StructuredTaskScope.open(Joiner, configFunction)`:
    ```java
    try (var scope = StructuredTaskScope.open(
            StructuredTaskScope.Joiner.<Double>awaitAllSuccessfulOrThrow(),
            cf -> cf.withName("risk-evaluation-scope")
                    .withTimeout(Duration.ofMillis(deadlineMillis)))) {

        var velocityTask = scope.fork(() -> evaluateAccountVelocity(request));
        var ruleTask     = scope.fork(() -> evaluateTenantRules(request));
        var anomalyTask  = scope.fork(() -> evaluateAnomalyHeuristics(request));

        scope.join();
        double result = velocityTask.get() + ruleTask.get() + anomalyTask.get();
    }
    ```
- **Scoped Values (`ScopedValue<T>` — JEP 506 Final in Java 25)**:
  - Bind via `ScopedValue.where(RequestContextHolder.CONTEXT, context).call(() -> ...)` or `.run(() -> ...)`.
  - Child virtual threads forked inside `StructuredTaskScope` automatically inherit the parent's `ScopedValue` bindings with zero copying.
- **Stream Gatherers (`Gatherers` — JEP 485 Final in Java 24/25)**:
  - Use `validEvents.stream().gather(Gatherers.windowFixed(microBatchWindowSize)).toList()` for in-stream micro-batching.
- **Records, Sealed Interfaces, Record Patterns & Unnamed Variables (`_`)**:
  - Model domain outcomes as `sealed interface` with `record` implementations and process them with exhaustive `switch` expressions (no `default:` branch) using unnamed pattern variables (`_`) for unused components.
- **Java 25 Classfile Version (`69.0`) & Architectural Governance**:
  - Java 25 produces classfile major version `69` (`65535` with `--enable-preview`), which breaks older shaded ASM libraries (such as `archunit:1.4.0`). Use Java 25 native reflection (`Class.isRecord()`, `Class.isSealed()`) or `java.lang.classfile.ClassFile` (JEP 484) in [`ArielArchitectureGovernanceTest`](../../tests/ariel-load-and-chaos-tests/src/test/java/io/ariel/chaos/ArielArchitectureGovernanceTest.java).

---

## 3. Verified Spring Boot 4.1.2 (Spring Framework 7 / Jackson 3) Specifics

- **Modular Starters in Spring Boot 4.1.2**:
  - Web MVC: `org.springframework.boot:spring-boot-starter-webmvc` (and `spring-boot-starter-webmvc-test` for `@AutoConfigureMockMvc` in tests).
  - Declarative HTTP / `RestClient`: `org.springframework.boot:spring-boot-starter-restclient`.
  - Flyway: `org.springframework.boot:spring-boot-starter-flyway` + `org.flywaydb:flyway-database-postgresql`.
  - OpenTelemetry: `org.springframework.boot:spring-boot-starter-opentelemetry`.
- **Jackson 3 (`tools.jackson.databind`)**:
  - Spring Boot 4.1.2 uses **Jackson 3** (`tools.jackson.databind.json.JsonMapper`) for serialization/deserialization, while annotations (`@JsonTypeInfo`, `@JsonSubTypes`) remain in `com.fasterxml.jackson.annotation.*`.
- **Spring Framework 7 `HttpHeaders` API Change**:
  - `org.springframework.http.HttpHeaders` no longer extends `MultiValueMap` (`addIfAbsent` is removed). Use `request.getHeaders().set(headerName, value)` inside `org.springframework.http.client.ClientHttpRequestInitializer`.
- **Spring Boot 4.1.2 Package Relocations**:
  - `@EntityScan`: `org.springframework.boot.persistence.autoconfigure.EntityScan`
  - `@AutoConfigureMockMvc`: `org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc`
  - Bean mocking in tests: `@MockitoBean` (`org.springframework.test.context.bean.override.mockito.MockitoBean`)
- **Test Isolation Best Practices**:
  - Always include `"management.otlp.metrics.export.enabled=false"` in `@SpringBootTest(properties = ...)` so tests do not attempt to push metrics to `localhost:4318` on context shutdown.
  - When testing `OutboxRelayWorker` or services with `@Scheduled` tasks, configure `initialDelayString = "${ariel.outbox.initial-delay-ms:2000}"` and mock `KafkaTemplate` / `StringRedisTemplate` via `@MockitoBean` in slice/integration tests so tests never block on offline local ports.
