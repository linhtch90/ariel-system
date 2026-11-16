package io.ariel.common.domain;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Inbound command payload for high-throughput financial/telemetry transaction ingestion.
 */
public record TransactionCommand(
        @NotBlank(message = "accountId is required")
        String accountId,

        @NotBlank(message = "counterpartyId is required")
        String counterpartyId,

        @NotNull(message = "amount is required")
        @DecimalMin(value = "0.01", message = "amount must be strictly positive")
        BigDecimal amount,

        @NotBlank(message = "currency is required")
        String currency,

        @NotBlank(message = "region is required")
        String region,

        String deviceFingerprint,

        Map<String, String> metadata
) {
    public TransactionCommand {
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
