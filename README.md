# Project Ariel — Cloud-Native High-Throughput Event, Risk & Transaction Platform

**Project Ariel** is a reference cloud-native distributed system engineered on the modern Java stack (**Java 25**, **Spring Boot v4.1.2**, **Gradle Groovy DSL**). It is designed for **99.99%+ High Availability**, **sub-15ms P99 Fast-Path Latency**, **elastic horizontal scaling**, **durable fault tolerance**, and **petabyte-scale event/data processing**.

---

## 1. System Concept & Use Case

Project Ariel acts as a **Real-Time Global Transaction, Risk Intelligence & Telemetry Engine** capable of handling extreme traffic bursts (e.g., instant payment clearing, real-time fraud detection, high-frequency IoT/financial telemetry, or flash-sale order orchestration):

1. **Ultra-Low Latency Synchronous Fast Path (<15ms P99)**:
   - Requests enter through **Ariel Edge Gateway** (Rate Limiting, OAuth2/JWT, mTLS) into the **Ingestion Service** (Java 25 Virtual Threads).
   - **Idempotency Barrier**: Deduplicates retries in `<1ms` using a **Two-Tier Near-Cache** (`L1 Caffeine` + `L2 Redis 8 / Valkey Cluster`).
   - **Parallel Risk Decisioning**: The **Risk Engine** uses **Java 25 Structured Concurrency (`StructuredTaskScope`)** and **Scoped Values (`ScopedValue`)** to evaluate velocity counters, rule graphs, and anomaly scores in parallel within a strict 5ms deadline.
   - **Atomic State + Event Durability**: The **Ledger Service** commits state changes and an **Outbox Event** in a single ACID transaction on **PostgreSQL 17**, preventing dual-write bugs.
2. **High-Throughput Asynchronous Event & Big Data Plane**:
   - Outbox events stream via **Debezium CDC / Relay** into **Apache Kafka (KRaft)** partitioned topics with **Avro/Protobuf Schema Registry**.
   - **Stream Processor** pods (autoscale-driven by **KEDA** on Kafka consumer lag) use **Java 25 Stream Gatherers** for micro-batching and fan out writes to:
     - **ScyllaDB / Apache Cassandra**: High-velocity raw time-series event storage.
     - **ClickHouse**: Real-time columnar OLAP store for sub-second analytics over billions of rows.
     - **Apache Iceberg (S3 / MinIO)**: Cold lakehouse storage for historical batch & AI/ML pipelines.
3. **CQRS Read Plane**:
   - **Query & Analytics Service** serves real-time materialized views and analytical queries backed by ClickHouse, ScyllaDB, and Redis read-through caches.

---

## 2. System Architecture Diagrams (PlantUML)

All architecture diagrams are maintained as code in [`docs/architecture/`](docs/architecture/) and compiled to SVG and PNG formats:

### 2.1 Overall Cloud-Native System Architecture
- Source: [`docs/architecture/system-architecture.puml`](docs/architecture/system-architecture.puml)
- Rendered: [`docs/architecture/system-architecture.svg`](docs/architecture/system-architecture.svg) | [`docs/architecture/system-architecture.png`](docs/architecture/system-architecture.png)

![System Architecture](docs/architecture/system-architecture.svg)

### 2.2 Low-Latency Request & Durable Event Sequence Flow
- Source: [`docs/architecture/data-flow-sequence.puml`](docs/architecture/data-flow-sequence.puml)
- Rendered: [`docs/architecture/data-flow-sequence.svg`](docs/architecture/data-flow-sequence.svg) | [`docs/architecture/data-flow-sequence.png`](docs/architecture/data-flow-sequence.png)

![Data Flow Sequence](docs/architecture/data-flow-sequence.svg)

### 2.3 Multi-AZ High Availability, Resilience & Horizontal Scaling Topology
- Source: [`docs/architecture/scaling-resilience-topology.puml`](docs/architecture/scaling-resilience-topology.puml)
- Rendered: [`docs/architecture/scaling-resilience-topology.svg`](docs/architecture/scaling-resilience-topology.svg) | [`docs/architecture/scaling-resilience-topology.png`](docs/architecture/scaling-resilience-topology.png)

![Scaling & Resilience Topology](docs/architecture/scaling-resilience-topology.svg)

---

## 3. AI-Native Project Structure (`.agents/` & `AGENTS.md`)

This repository follows best practices for AI-assisted software engineering:

```text
ariel-system/
├── AGENTS.md                                      # Root AI Agent Constitution & Architecture Rules
├── README.md                                      # Project Overview & Architecture Guide
├── .agents/
│   ├── rules/
│   │   └── java25-springboot4-standards.md        # Java 25, Spring Boot 4.1.2 & Gradle Groovy Rules
│   ├── agents/                                    # Specialized AI Subagent Personas
│   │   ├── cloud-architect.md                     # Principal Cloud-Native System Architect
│   │   ├── java-microservice-engineer.md          # Staff Java 25 & Spring Boot 4.1.2 Engineer
│   │   ├── data-streaming-specialist.md           # Principal Cache, Kafka & Polyglot Data Engineer
│   │   └── sre-resilience-auditor.md              # Principal SRE, Autoscaling & Chaos Engineer
│   ├── skills/                                    # On-Demand Procedural Skills (SKILL.md)
│   │   ├── scaffold-spring-service/SKILL.md       # Scaffolds Java 25 + Spring Boot 4.1.2 services
│   │   ├── implement-caching-messaging/SKILL.md   # L1/L2 Near-Cache + Outbox + Kafka DLQ patterns
│   │   ├── resilience-and-scaling-audit/SKILL.md  # Resilience4j + KEDA/HPA + ZGC tuning checklist
│   │   └── plantuml-architecture-sync/SKILL.md    # Updates & renders PlantUML diagrams
│   └── prompts/                                   # Reusable Project & Phase Prompts
│       ├── 00-master-project-prompt.md            # Master System Prompt for Project Ariel
│       ├── 01-bootstrap-gradle-monorepo.md        # Phase 1: Gradle Groovy Monorepo & Infra
│       ├── 02-implement-core-services.md          # Phase 2: Fast-Path Microservices
│       ├── 03-implement-data-and-streaming.md     # Phase 3: Kafka Streaming & Polyglot Data
│       └── 04-load-testing-and-chaos-engineering.md # Phase 4: K8s HPA/KEDA & Chaos Testing
└── docs/
    └── architecture/                              # PlantUML (.puml) sources + .svg & .png diagrams
        ├── system-architecture.puml
        ├── data-flow-sequence.puml
        └── scaling-resilience-topology.puml
```
