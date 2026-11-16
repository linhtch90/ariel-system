package io.ariel.common.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Standardized response returned by Ingestion Service after synchronous fast-path processing.
 */
public record TransactionReceipt(
        String transactionId,
        String correlationId,
        String tenantId,
        String accountId,
        BigDecimal amount,
        String currency,
        String status,
        double riskScore,
        boolean idempotentReplay,
        long processingMillis,
        Instant processedAt
) {
}
