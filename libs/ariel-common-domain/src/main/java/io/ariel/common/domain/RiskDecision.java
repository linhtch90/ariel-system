package io.ariel.common.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Java 25 sealed hierarchy representing the exhaustive outcome of a real-time risk evaluation.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "status")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RiskDecision.Approved.class, name = "APPROVED"),
        @JsonSubTypes.Type(value = RiskDecision.Rejected.class, name = "REJECTED"),
        @JsonSubTypes.Type(value = RiskDecision.ManualReviewRequired.class, name = "MANUAL_REVIEW")
})
public sealed interface RiskDecision
        permits RiskDecision.Approved, RiskDecision.Rejected, RiskDecision.ManualReviewRequired {

    double riskScore();

    long evaluationMicros();

    record Approved(
            double riskScore,
            String routingTier,
            long evaluationMicros
    ) implements RiskDecision {
    }

    record Rejected(
            double riskScore,
            String reasonCode,
            String description,
            long evaluationMicros
    ) implements RiskDecision {
    }

    record ManualReviewRequired(
            double riskScore,
            String reviewQueue,
            String reason,
            long evaluationMicros
    ) implements RiskDecision {
    }
}
