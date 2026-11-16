---
name: scaffold-spring-service
description: >-
  Use this skill when creating or scaffolding a new microservice module in the
  Ariel System using Java 25, Spring Boot 4.1.2, and Gradle 9.5 Groovy DSL.
---

# Skill: Scaffold Spring Boot 4.1.2 & Java 25 Microservice

Follow this workflow whenever a new service module (e.g., `services/ariel-notification-service`) is added to the repository.

## Step 1: Register Module in `settings.gradle`
1. Open the root [`settings.gradle`](../../../settings.gradle) file.
2. Add `include 'services:<service-name>'` under the `services:*` section.

## Step 2: Create `build.gradle` Using the Shared Convention Plugin
Create `services/<service-name>/build.gradle` applying the `ariel.spring-boot-service` convention plugin from `buildSrc` (which automatically configures Java 25, `--enable-preview`, `-XX:+UseZGC`, `spring-boot-starter-webmvc`, `actuator`, `validation`, `opentelemetry`, `prometheus`, and JUnit 5 + `webmvc-test`):

```groovy
plugins {
    id 'ariel.spring-boot-service'
}

dependencies {
    implementation project(':libs:ariel-common-domain')
    implementation project(':libs:ariel-observability-starter')
    implementation project(':libs:ariel-cache-starter')
    implementation libs.spring.boot.starter.restclient
}
```

## Step 3: Create Clean Package Layout
Create the standard package structure under `src/main/java/io/ariel/<service>/`:
- `web/` — `@RestController` endpoints (must reside in `..web` and never inject `Repository` interfaces directly to pass `ArielArchitectureGovernanceTest`).
- `usecase/` or `service/` — Business logic using constructor injection (never `@Autowired` field injection), Java 25 `StructuredTaskScope.open(...)`, and `RequestContextHolder.current()`.
- `client/` — Outbound declarative `@HttpExchange` interfaces backed by `RestClient`.
- `domain/` — Persistence entities (`@Entity`) and Spring Data repositories (`JpaRepository`).

## Step 4: Configure `src/main/resources/application.yml`
Ensure Virtual Threads, Graceful Shutdown, and Actuator probes are enabled:
```yaml
server:
  port: 8086
  shutdown: graceful

spring:
  application:
    name: <service-name>
  threads:
    virtual:
      enabled: true
  lifecycle:
    timeout-per-shutdown-phase: 30s

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

## Step 5: Register in `ArielArchitectureGovernanceTest` & Verify Build
1. Add the new controller and service classes to [`ArielArchitectureGovernanceTest.java`](../../../tests/ariel-load-and-chaos-tests/src/test/java/io/ariel/chaos/ArielArchitectureGovernanceTest.java).
2. Run `./gradlew :services:<service-name>:check :tests:ariel-load-and-chaos-tests:test` to verify compilation, unit/integration tests, and architectural governance.
