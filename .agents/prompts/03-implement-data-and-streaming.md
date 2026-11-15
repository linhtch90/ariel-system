# Phase 3 Prompt — Implement High-Volume Event Streaming & Polyglot Data Plane

```markdown
Act as `data-streaming-specialist` and activate the `implement-caching-messaging` skill.

Task: Implement the asynchronous event streaming and large-scale data handling components:
1. Outbox CDC / Relay worker publishing validated ledger events to Apache Kafka (`ariel.transactions.validated`) with Avro/Protobuf serialization and `acks=all`.
2. `services/ariel-stream-processor`:
   - High-throughput consumer group using Java 25 `Stream.gather(Gatherers.windowFixed(...))` for micro-batching.
   - Parallel sink writers persisting raw event logs into **ScyllaDB**, real-time analytical rollups into **ClickHouse**, and rolling counters into **Redis**.
   - Non-blocking retry topics and Dead Letter Queue (`ariel.dlq`) handler for poison pills.
3. `services/ariel-query-service`:
   - CQRS read endpoints querying ClickHouse for real-time analytical aggregations and ScyllaDB/PostgreSQL for point-in-time lookups, fronted by the Two-Tier Near-Cache.
```
