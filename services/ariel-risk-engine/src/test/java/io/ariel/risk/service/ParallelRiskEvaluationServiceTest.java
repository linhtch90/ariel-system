package io.ariel.risk.service;

import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.RiskEvaluationRequest;
import java.math.BigDecimal;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class ParallelRiskEvaluationServiceTest {

    private final TwoTierNearCache nearCache = new TwoTierNearCache(
            null, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10)
    );

    private final ParallelRiskEvaluationService service = new ParallelRiskEvaluationService(
            nearCache,
            new StaticListableBeanFactory().getBeanProvider(StringRedisTemplate.class),
            100L
    );

    @Test
    void approvesLowRiskFastLaneTransactionInParallel() {
        var req = new RiskEvaluationRequest(
                "corr-1", "tenant-1", "acc-1", "merchant-1",
                new BigDecimal("120.50"), "USD", "US-EAST", "device-ok"
        );

        RiskDecision decision = service.evaluate(req);
        assertThat(decision).isInstanceOf(RiskDecision.Approved.class);
        assertThat(decision.riskScore()).isLessThan(0.55);
    }

    @Test
    void routesLargeTransactionToManualReviewQueue() {
        var req = new RiskEvaluationRequest(
                "corr-2", "tenant-1", "acc-2", "merchant-1",
                new BigDecimal("75000.00"), "USD", "US-EAST", "device-ok"
        );

        RiskDecision decision = service.evaluate(req);
        assertThat(decision).isInstanceOf(RiskDecision.ManualReviewRequired.class);
    }

    @Test
    void rejectsTransactionExceedingHardLimit() {
        var req = new RiskEvaluationRequest(
                "corr-3", "tenant-1", "acc-3", "merchant-1",
                new BigDecimal("2500000.00"), "USD", "US-EAST", "device-ok"
        );

        RiskDecision decision = service.evaluate(req);
        assertThat(decision).isInstanceOf(RiskDecision.Rejected.class);
    }

    @Test
    void escalatesScoreWhenTenantIsRestrictedAndAccountVelocitySpikesWithMissingDeviceFingerprint() {
        nearCache.put("ariel:rules:tenant:tenant-restricted", "RESTRICTED_TIER");

        // Accumulate > $500,000 velocity on the same account with missing device fingerprint
        RiskDecision finalDecision = null;
        for (int i = 0; i < 15; i++) {
            finalDecision = service.evaluate(new RiskEvaluationRequest(
                    "corr-vel-" + i, "tenant-restricted", "acc-velocity-spike", "merchant-1",
                    new BigDecimal("45000.00"), "USD", "SANCTIONED_REGION", ""
            ));
        }

        assertThat(finalDecision).isInstanceOf(RiskDecision.Rejected.class);
        assertThat(finalDecision.riskScore()).isGreaterThanOrEqualTo(0.85);
    }
}
