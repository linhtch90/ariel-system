package io.ariel.common.context;

import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.TransactionCommand;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.StructuredTaskScope;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestContextHolderTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

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
    void unboundScopedValueReturnsNormalizedSystemDefaultAndNormalizesBlankFields() {
        RequestContext unbound = RequestContextHolder.current();
        assertThat(unbound.tenantId()).isEqualTo("default-tenant");
        assertThat(unbound.correlationId()).isNotBlank();
        assertThat(unbound.idempotencyKey()).isEqualTo(unbound.correlationId());

        var normalized = new RequestContext("  ", "  ", "  ", null);
        assertThat(normalized.tenantId()).isEqualTo("default-tenant");
        assertThat(normalized.correlationId()).isNotBlank();
        assertThat(normalized.idempotencyKey()).isEqualTo(normalized.correlationId());
        assertThat(normalized.receivedAt()).isNotNull();
    }

    @Test
    void transactionCommandEnforcesDefensiveImmutabilityOnMetadataMap() {
        Map<String, String> mutable = new HashMap<>();
        mutable.put("env", "prod");

        var cmd = new TransactionCommand(
                "acc-1", "merch-1", new BigDecimal("10.00"), "USD", "US-EAST", "fp-1", mutable
        );
        mutable.put("tampered", "true");

        assertThat(cmd.metadata()).containsOnlyKeys("env");
        assertThatThrownBy(() -> cmd.metadata().put("hack", "1"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void polymorphicJsonSerializationRoundTripForAllSealedRiskDecisionSubtypes() {
        RiskDecision approved = new RiskDecision.Approved(0.04, "FAST_LANE", 850L);
        RiskDecision rejected = new RiskDecision.Rejected(0.95, "SANCTIONED", "Blocked region", 410L);
        RiskDecision review = new RiskDecision.ManualReviewRequired(0.62, "TIER_2", "High value", 520L);

        String approvedJson = jsonMapper.writeValueAsString(approved);
        String rejectedJson = jsonMapper.writeValueAsString(rejected);
        String reviewJson = jsonMapper.writeValueAsString(review);

        assertThat(approvedJson).contains("\"status\":\"APPROVED\"");
        assertThat(rejectedJson).contains("\"status\":\"REJECTED\"");
        assertThat(reviewJson).contains("\"status\":\"MANUAL_REVIEW\"");

        assertThat(jsonMapper.readValue(approvedJson, RiskDecision.class)).isEqualTo(approved);
        assertThat(jsonMapper.readValue(rejectedJson, RiskDecision.class)).isEqualTo(rejected);
        assertThat(jsonMapper.readValue(reviewJson, RiskDecision.class)).isEqualTo(review);
    }
}
