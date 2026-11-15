# Java 25, Spring Boot 4.1.2 & Gradle Groovy Engineering Standards

## 1. Gradle Groovy Build Conventions (`build.gradle`)

- All modules MUST use the Groovy DSL (`build.gradle` and `settings.gradle`).
- Use `gradle/libs.versions.toml` for centralized version management:
  - `springBoot = "4.1.2"`
  - `java = "25"`
- Enforce Java 25 toolchain in root/convention configuration:
  ```groovy
  java {
      toolchain {
          languageVersion = JavaLanguageVersion.of(25)
      }
  }
  ```
- Enable preview or incubator features only when required, and configure JVM args for tests and bootRun:
  ```groovy
  tasks.withType(JavaCompile).configureEach {
      options.encoding = 'UTF-8'
      options.compilerArgs.addAll(['-parameters'])
  }

  tasks.withType(Test).configureEach {
      useJUnitPlatform()
      jvmArgs '-XX:+UseZGC', '-XX:+ZGenerational'
  }
  ```

## 2. Modern Java 25 Idioms

- **Immutability First**: All DTOs, Domain Events, Commands, Queries, and Value Objects MUST be `record` types with compact constructors for validation.
- **Exhaustive Domain Modeling**: Model domain states and command outcomes using `sealed interface` hierarchies and exhaustive `switch` expressions with pattern matching (no default branch hiding unhandled subtypes).
- **Concurrency**:
  - Never block platform carrier threads with synchronized blocks around I/O; use `ReentrantLock` instead of `synchronized` to avoid virtual thread pinning (or rely on Java 24+/25 JEP 491 synchronized virtual thread unpinning while still preferring explicit locks for fairness/timeouts).
  - Use `StructuredTaskScope.ShutdownOnFailure` or `ShutdownOnSuccess` when aggregating multiple downstream service calls (e.g., Risk Engine querying Redis velocity counters + Fraud rules in parallel).
  - Use `ScopedValue<T>` for request-scoped context propagation (`CorrelationId`, `TenantId`, `CallerPrincipal`).
- **Stream Processing**: Use `Stream.gather(Gatherers.windowFixed(...))` or custom `Gatherer` implementations for in-memory batching and sliding-window transformations.

## 3. Spring Boot 4.1.2 Best Practices

- **Virtual Threads**: Always enable virtual threads in `application.yml`:
  ```yaml
  spring:
    threads:
      virtual:
        enabled: true
    lifecycle:
      timeout-per-shutdown-phase: 30s
  server:
    shutdown: graceful
  ```
- **API Contracts & Error Handling**:
  - Return `ResponseEntity` with RFC 9457 `ProblemDetail` for all error responses via `@RestControllerAdvice`.
  - Use Jakarta Bean Validation 3.1 (`@Valid`, custom constraint validators) at the controller/gRPC boundary.
- **Observability**:
  - Expose Prometheus & OTLP metrics via Micrometer (`micrometer-registry-prometheus`, `micrometer-tracing-bridge-otel`).
  - Annotate critical domain operations with `@Observed` to automatically emit both timer metrics and distributed trace spans.
- **Testing**:
  - Use **Testcontainers** (`@ServiceConnection`) for integration tests against real PostgreSQL, Redis, Kafka, ScyllaDB, and ClickHouse containers.
  - Use **ArchUnit** to enforce hexagonal/clean architecture boundaries within each microservice.
