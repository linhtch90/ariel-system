---
name: scaffold-spring-service
description: >-
  Use this skill when creating or scaffolding a new microservice module in the
  Ariel System using Java 25, Spring Boot 4.1.2, and Gradle Groovy DSL.
---

# Skill: Scaffold Spring Boot 4.1.2 & Java 25 Microservice

Follow this workflow whenever a new service module (e.g., `services/ariel-ingestion-service`) is added to the repository.

## Step 1: Register Module in `settings.gradle`
1. Open the root `settings.gradle` file.
2. Add `include 'services:<service-name>'` in alphabetical order.

## Step 2: Create `build.gradle` (Groovy DSL)
Create `services/<service-name>/build.gradle` applying the project's Java 25 and Spring Boot 4.1.2 conventions:

```groovy
plugins {
    id 'java'
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

dependencies {
    implementation project(':libs:ariel-common-domain')
    implementation project(':libs:ariel-observability-starter')

    implementation libs.spring.boot.starter.web
    implementation libs.spring.boot.starter.actuator
    implementation libs.spring.boot.starter.validation
    implementation libs.resilience4j.spring.boot3
    implementation libs.micrometer.registry.prometheus
    implementation libs.micrometer.tracing.bridge.otel

    testImplementation libs.spring.boot.starter.test
    testImplementation libs.spring.boot.testcontainers
    testImplementation libs.testcontainers.junit.jupiter
}

tasks.named('bootRun') {
    jvmArgs = ['-XX:+UseZGC', '-XX:+ZGenerational']
}
```

## Step 3: Create Hexagonal Package Structure
Create the standard clean/hexagonal package layout under `src/main/java/io/ariel/<service>/`:
- `domain/model/` — Java 25 `record` entities, value objects, and `sealed interface` state machines.
- `domain/port/in/` — Inbound use-case interfaces.
- `domain/port/out/` — Outbound repository, cache, and event publisher interfaces.
- `application/usecase/` — Use-case implementations using `StructuredTaskScope` and `ScopedValue`.
- `infrastructure/adapter/in/web/` — REST / gRPC controllers & `ProblemDetail` exception handlers.
- `infrastructure/adapter/out/` — Persistence, Redis L1/L2 cache, and Kafka adapters.
- `infrastructure/config/` — Spring `@Configuration` classes.

## Step 4: Configure `src/main/resources/application.yml`
Ensure Virtual Threads, Graceful Shutdown, and Actuator probes are enabled by default:
```yaml
spring:
  application:
    name: <service-name>
  threads:
    virtual:
      enabled: true
  lifecycle:
    timeout-per-shutdown-phase: 30s

server:
  shutdown: graceful

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      probes:
        enabled: true
      show-details: always
```

## Step 5: Verify Build & Architecture Test
Run `./gradlew :services:<service-name>:check` to verify compilation on Java 25 and pass ArchUnit / Testcontainers checks.
