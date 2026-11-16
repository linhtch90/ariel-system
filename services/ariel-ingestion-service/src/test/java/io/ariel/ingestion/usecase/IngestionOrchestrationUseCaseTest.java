package io.ariel.ingestion.usecase;

import io.ariel.cache.RedisIdempotencyService;
import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.context.RequestContext;
import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.TransactionReceipt;
import io.ariel.ingestion.client.FastPathClientsConfig.LedgerServiceClient;
import io.ariel.ingestion.client.FastPathClientsConfig.RiskEngineClient;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class IngestionOrchestrationUseCaseTest {

    @Test
    void processesApprovedTransactionAndReplaysIdempotentlyOnDuplicateRequest() throws Exception {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(5), Duration.ofHours(1));
        var idempotencyService = new RedisIdempotencyService(nearCache, null);
        var ledgerCallCount = new AtomicInteger(0);

        RiskEngineClient riskClient = _ -> new RiskDecision.Approved(0.05, "FAST_LANE", 400L);
        LedgerServiceClient ledgerClient = cmd -> {
            ledgerCallCount.incrementAndGet();
            return new LedgerEntryDto(
                    "tx-fixed-001",
                    cmd.correlationId(),
                    cmd.tenantId(),
                    cmd.accountId(),
                    cmd.counterpartyId(),
                    cmd.amount(),
                    cmd.currency(),
                    cmd.region(),
                    cmd.riskScore(),
                    new BigDecimal("1000.00"),
                    Instant.now()
            );
        };

        var useCase = new IngestionOrchestrationUseCase(
                idempotencyService,
                riskClient,
                ledgerClient,
                JsonMapper.builder().build()
        );

        var command = new TransactionCommand(
                "acc-100",
                "merchant-200",
                new BigDecimal("250.00"),
                "USD",
                "US-EAST",
                "fp-device-1",
                Map.of("channel", "mobile")
        );

        var ctx = new RequestContext("corr-1", "tenant-acme", "idem-unique-777", Instant.now());

        // First call executes Risk + Ledger
        TransactionReceipt first = RequestContextHolder.callWith(ctx, () -> useCase.processTransaction(command));
        assertThat(first.transactionId()).isEqualTo("tx-fixed-001");
        assertThat(first.idempotentReplay()).isFalse();
        assertThat(ledgerCallCount.get()).isEqualTo(1);

        // Second call with same Idempotency-Key returns cached receipt without calling Risk or Ledger
        TransactionReceipt replay = RequestContextHolder.callWith(ctx, () -> useCase.processTransaction(command));
        assertThat(replay.transactionId()).isEqualTo("tx-fixed-001");
        assertThat(replay.idempotentReplay()).isTrue();
        assertThat(ledgerCallCount.get()).isEqualTo(1);
    }
}
