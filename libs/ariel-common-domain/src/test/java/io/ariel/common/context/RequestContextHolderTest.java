package io.ariel.common.context;

import io.ariel.common.domain.RiskDecision;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.StructuredTaskScope;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RequestContextHolderTest {

    @Test
    void scopedValuePropagatesAutomaticallyIntoStructuredTaskScopeVirtualThreads() throws Exception {
        var ctx = new RequestContext("corr-123", "tenant-acme", "idem-999", Instant.now());

        String result = RequestContextHolder.callWith(ctx, () -> {
            try (var scope = StructuredTaskScope.open(
                    StructuredTaskScope.Joiner.<String>awaitAllSuccessfulOrThrow(),
                    cf -> cf.withName("test-scope").withTimeout(Duration.ofSeconds(2)))) {

                var task1 = scope.fork(() -> RequestContextHolder.current().tenantId()
                        + ":" + Thread.currentThread().isVirtual());
                var task2 = scope.fork(() -> RequestContextHolder.current().idempotencyKey());

                scope.join();
                return task1.get() + "|" + task2.get();
            }
        });

        assertThat(result).isEqualTo("tenant-acme:true|idem-999");
    }

    @Test
    void exhaustivePatternMatchingOverSealedRiskDecision() {
        RiskDecision decision = new RiskDecision.Approved(0.04, "FAST_LANE", 850L);

        String summary = switch (decision) {
            case RiskDecision.Approved(var score, var tier, _) -> "APPROVED:" + tier + ":" + score;
            case RiskDecision.Rejected(_, var code, _, _) -> "REJECTED:" + code;
            case RiskDecision.ManualReviewRequired(_, var queue, _, _) -> "REVIEW:" + queue;
        };

        assertThat(summary).isEqualTo("APPROVED:FAST_LANE:0.04");
    }
}
