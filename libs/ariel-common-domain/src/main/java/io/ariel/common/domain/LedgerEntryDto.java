package io.ariel.common.domain;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Command & response DTO for the ACID Transaction Ledger Service.
 */
public record LedgerEntryDto(
        String transactionId,
        @NotBlank String correlationId,
        @NotBlank String tenantId,
        @NotBlank String accountId,
        @NotBlank String counterpartyId,
        @NotNull @DecimalMin("0.01") BigDecimal amount,
        @NotBlank String currency,
        @NotBlank String region,
        double riskScore,
        BigDecimal newBalance,
        Instant committedAt
) {
}
