package io.ariel.risk.service;

import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.RiskEvaluationRequest;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.DoubleAdder;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Real-Time Risk Engine using Java 25 {@link StructuredTaskScope} to execute 3 sub-checks
 * concurrently across Virtual Threads within a strict deadline:
 * <ol>
 *   <li>Account spending velocity check (Redis L2 / in-memory rolling counter)</li>
 *   <li>Tenant rule graph evaluation (Caffeine L1 + Redis L2 Near-Cache)</li>
 *   <li>Heuristic amount/region/device anomaly score</li>
 * </ol>
 */
@Service
public class ParallelRiskEvaluationService {

    private static final BigDecimal HARD_LIMIT = new BigDecimal("1000000.00");
    private static final BigDecimal REVIEW_THRESHOLD = new BigDecimal("50000.00");

    private final TwoTierNearCache nearCache;
    private final StringRedisTemplate redisTemplate;
    private final long deadlineMillis;
    private final ConcurrentHashMap<String, DoubleAdder> localVelocityCounters = new ConcurrentHashMap<>();

    public ParallelRiskEvaluationService(
            TwoTierNearCache nearCache,
            ObjectProvider<StringRedisTemplate> redisTemplateProvider,
            @Value("${ariel.risk.deadline-ms:50}") long deadlineMillis
    ) {
        this.nearCache = nearCache;
        this.redisTemplate = redisTemplateProvider.getIfAvailable();
        this.deadlineMillis = deadlineMillis;
    }

    public RiskDecision evaluate(RiskEvaluationRequest request) {
        long startNanos = System.nanoTime();

        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<Double>awaitAllSuccessfulOrThrow(),
                cf -> cf.withName("risk-evaluation-scope")
                        .withTimeout(Duration.ofMillis(deadlineMillis)))) {

            // Subtask 1: Velocity Check (Redis / rolling counter)
            var velocityTask = scope.fork(() -> evaluateAccountVelocity(request));

            // Subtask 2: Tenant Rule Graph Check (L1 Caffeine -> L2 Redis Near-Cache)
            var ruleTask = scope.fork(() -> evaluateTenantRules(request));

            // Subtask 3: Heuristic Anomaly Check
            var anomalyTask = scope.fork(() -> evaluateAnomalyHeuristics(request));

            scope.join();

            double combinedScore = Math.min(1.0,
                    (velocityTask.get() * 0.35) + (ruleTask.get() * 0.35) + (anomalyTask.get() * 0.30)
            );
            long elapsedMicros = Duration.ofNanos(System.nanoTime() - startNanos).toNanos() / 1_000L;

            if (request.amount().compareTo(HARD_LIMIT) > 0 || combinedScore >= 0.85) {
                return new RiskDecision.Rejected(
                        combinedScore,
                        "HIGH_RISK_THRESHOLD_EXCEEDED",
                        "Transaction blocked by real-time risk policy",
                        elapsedMicros
                );
            }

            if (request.amount().compareTo(REVIEW_THRESHOLD) > 0 || combinedScore >= 0.55) {
                return new RiskDecision.ManualReviewRequired(
                        combinedScore,
                        "TIER_2_COMPLIANCE_QUEUE",
                        "Amount or risk score exceeds automated fast-lane threshold",
                        elapsedMicros
                );
            }

            return new RiskDecision.Approved(combinedScore, "FAST_LANE", elapsedMicros);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long elapsedMicros = Duration.ofNanos(System.nanoTime() - startNanos).toNanos() / 1_000L;
            return new RiskDecision.ManualReviewRequired(
                    0.60,
                    "TIMEOUT_FALLBACK_QUEUE",
                    "Risk evaluation interrupted or timed out",
                    elapsedMicros
            );
        } catch (Exception e) {
            long elapsedMicros = Duration.ofNanos(System.nanoTime() - startNanos).toNanos() / 1_000L;
            return new RiskDecision.ManualReviewRequired(
                    0.65,
                    "DEGRADED_FALLBACK_QUEUE",
                    "Fallback triggered: " + e.getMessage(),
                    elapsedMicros
            );
        }
    }

    private double evaluateAccountVelocity(RiskEvaluationRequest request) {
        String key = "ariel:velocity:" + request.tenantId() + ":" + request.accountId();
        double currentVolume;
        if (redisTemplate != null) {
            try {
                Double updated = redisTemplate.opsForValue().increment(key, request.amount().doubleValue());
                currentVolume = updated != null ? updated : request.amount().doubleValue();
            } catch (Exception _) {
                currentVolume = incrementLocalVelocity(key, request.amount().doubleValue());
            }
        } else {
            currentVolume = incrementLocalVelocity(key, request.amount().doubleValue());
        }
        return currentVolume > 500_000.0 ? 0.90 : Math.min(0.50, currentVolume / 1_000_000.0);
    }

    private double incrementLocalVelocity(String key, double delta) {
        DoubleAdder adder = localVelocityCounters.computeIfAbsent(key, _ -> new DoubleAdder());
        adder.add(delta);
        return adder.sum();
    }

    private double evaluateTenantRules(RiskEvaluationRequest request) {
        String ruleKey = "ariel:rules:tenant:" + request.tenantId();
        String ruleConfig = nearCache.getOrLoad(ruleKey, _ -> "STANDARD_TIER");
        if ("RESTRICTED_TIER".equals(ruleConfig)) {
            return 0.95;
        }
        return "SANCTIONED_REGION".equalsIgnoreCase(request.region()) ? 1.0 : 0.05;
    }

    private double evaluateAnomalyHeuristics(RiskEvaluationRequest request) {
        double score = 0.02;
        if (request.deviceFingerprint() == null || request.deviceFingerprint().isBlank()) {
            score += 0.25;
        }
        if (request.amount().compareTo(new BigDecimal("25000.00")) > 0) {
            score += 0.35;
        }
        return Math.min(1.0, score);
    }
}
