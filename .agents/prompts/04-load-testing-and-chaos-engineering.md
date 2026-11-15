# Phase 4 Prompt — Resilience Audit, Kubernetes Autoscaling & Chaos Engineering

```markdown
Act as `sre-resilience-auditor` and activate the `resilience-and-scaling-audit` skill.

Task: Harden **Project Ariel** for production cloud-native operations:
1. Audit all services for Resilience4j CircuitBreaker, SemaphoreBulkhead, TimeLimiter, and Exponential Backoff with Jitter.
2. Create Kubernetes deployment manifests under `deploy/k8s/`:
   - Multi-AZ PodAntiAffinity, TopologySpreadConstraints, Readiness/Liveness/Startup probes, and graceful termination hooks.
   - `HorizontalPodAutoscaler` (HPA) for `ariel-edge-gateway`, `ariel-ingestion-service`, `ariel-risk-engine`, and `ariel-query-service`.
   - KEDA `ScaledObject` for `ariel-stream-processor` driven by Apache Kafka consumer lag.
3. Create Gatling / k6 load test suites in `tests/load-tests/` simulating 50,000+ RPS bursts and Toxiproxy chaos tests verifying zero data loss during Redis or Kafka broker outages.
```
