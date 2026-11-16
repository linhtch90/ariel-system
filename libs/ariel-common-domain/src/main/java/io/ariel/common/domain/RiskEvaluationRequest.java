package io.ariel.common.domain;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

/**
 * Request sent from Ingestion Service to Real-Time Risk Engine over @HttpExchange.
 */
public record RiskEvaluationRequest(
        @NotBlank String correlationId,
        @NotBlank String tenantId,
        @NotBlank String accountId,
        @NotBlank String counterpartyId,
        @NotNull @DecimalMin("0.01") BigDecimal amount,
        @NotBlank String currency,
        @NotBlank String region,
        String deviceFingerprint
) {
}
