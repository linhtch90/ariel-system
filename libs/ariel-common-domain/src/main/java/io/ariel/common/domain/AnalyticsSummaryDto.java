package io.ariel.common.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Real-time analytical rollup returned by the CQRS Query & Analytics Service.
 */
public record AnalyticsSummaryDto(
        String tenantId,
        long totalTransactions,
        BigDecimal totalVolume,
        double averageRiskScore,
        double p99RiskScore,
        long highRiskCount,
        Instant generatedAt
) {
}
