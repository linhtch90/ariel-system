package io.ariel.common.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Durable event payload emitted via the Transactional Outbox to Apache Kafka topic
 * {@code ariel.transactions.validated}.
 */
public record ValidatedTransactionEvent(
        String eventId,
        String transactionId,
        String correlationId,
        String tenantId,
        String accountId,
        String counterpartyId,
        BigDecimal amount,
        String currency,
        String region,
        double riskScore,
        BigDecimal resultingBalance,
        Instant occurredAt
) {
}
