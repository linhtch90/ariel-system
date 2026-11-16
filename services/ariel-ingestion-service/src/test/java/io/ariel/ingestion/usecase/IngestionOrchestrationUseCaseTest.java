package io.ariel.ingestion.usecase;

import io.ariel.cache.RedisIdempotencyService;
import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.context.RequestContext;
import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.TransactionReceipt;
import io.ariel.common.error.ArielBusinessException;
import io.ariel.ingestion.client.FastPathClientsConfig.LedgerServiceClient;
import io.ariel.ingestion.client.FastPathClientsConfig.RiskEngineClient;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IngestionOrchestrationUseCaseTest {

    private static final TransactionCommand SAMPLE_COMMAND = new TransactionCommand(
            "acc-100",
            "merchant-200",
            new BigDecimal("250.00"),
            "USD",
            "US-EAST",
            "fp-device-1",
            Map.of("channel", "mobile")
    );

    @Test
    void processesApprovedTransactionAndReplaysIdempotentlyOnDuplicateRequest() throws Exception {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(5), Duration.ofHours(1));
        var idempotencyService = new RedisIdempotencyService(nearCache, null);
        var ledgerCallCount = new AtomicInteger(0);

        RiskEngineClient riskClient = _ -> new RiskDecision.Approved(0.05, "FAST_LANE", 400L);
        LedgerServiceClient ledgerClient = cmd -> {
            ledgerCallCount.incrementAndGet();
            return new LedgerEntryDto(
                    "tx-fixed-001", cmd.correlationId(), cmd.tenantId(), cmd.accountId(),
                    cmd.counterpartyId(), cmd.amount(), cmd.currency(), cmd.region(),
                    cmd.riskScore(), new BigDecimal("1000.00"), Instant.now()
            );
        };

        var useCase = new IngestionOrchestrationUseCase(
                idempotencyService, riskClient, ledgerClient, JsonMapper.builder().build()
        );
        var ctx = new RequestContext("corr-1", "tenant-acme", "idem-unique-777", Instant.now());

        // First call executes Risk + Ledger
        TransactionReceipt first = RequestContextHolder.callWith(ctx, () -> useCase.processTransaction(SAMPLE_COMMAND));
        assertThat(first.transactionId()).isEqualTo("tx-fixed-001");
        assertThat(first.idempotentReplay()).isFalse();
        assertThat(ledgerCallCount.get()).isEqualTo(1);

        // Second call with same Idempotency-Key returns cached receipt without calling Risk or Ledger
        TransactionReceipt replay = RequestContextHolder.callWith(ctx, () -> useCase.processTransaction(SAMPLE_COMMAND));
        assertThat(replay.transactionId()).isEqualTo("tx-fixed-001");
        assertThat(replay.idempotentReplay()).isTrue();
        assertThat(ledgerCallCount.get()).isEqualTo(1);
    }

    @Test
    void manualReviewDecisionSkipsLedgerCommitAndCachesReviewReceipt() throws Exception {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(5), Duration.ofHours(1));
        var idempotencyService = new RedisIdempotencyService(nearCache, null);
        var ledgerCallCount = new AtomicInteger(0);

        RiskEngineClient riskClient = _ -> new RiskDecision.ManualReviewRequired(
                0.68, "TIER_2_COMPLIANCE", "High amount", 500L
        );
        LedgerServiceClient ledgerClient = cmd -> {
            ledgerCallCount.incrementAndGet();
            return cmd;
        };

        var useCase = new IngestionOrchestrationUseCase(
                idempotencyService, riskClient, ledgerClient, JsonMapper.builder().build()
        );
        var ctx = new RequestContext("corr-rev", "tenant-acme", "idem-review-1", Instant.now());

        TransactionReceipt receipt = RequestContextHolder.callWith(ctx, () -> useCase.processTransaction(SAMPLE_COMMAND));
        assertThat(receipt.status()).isEqualTo("QUEUED_FOR_MANUAL_REVIEW:TIER_2_COMPLIANCE");
        assertThat(ledgerCallCount.get()).isZero();
    }

    @Test
    void rejectedRiskDecisionThrows422NeverCallsLedgerAndReleasesIdempotencyLock() {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(5), Duration.ofHours(1));
        var idempotencyService = new RedisIdempotencyService(nearCache, null);
        var ledgerCallCount = new AtomicInteger(0);

        RiskEngineClient riskClient = _ -> new RiskDecision.Rejected(
                0.98, "SANCTIONS_HIT", "Blocked jurisdiction", 300L
        );
        LedgerServiceClient ledgerClient = cmd -> {
            ledgerCallCount.incrementAndGet();
            return cmd;
        };

        var useCase = new IngestionOrchestrationUseCase(
                idempotencyService, riskClient, ledgerClient, JsonMapper.builder().build()
        );
        var ctx = new RequestContext("corr-rej", "tenant-acme", "idem-reject-1", Instant.now());

        assertThatThrownBy(() -> RequestContextHolder.callWith(ctx, () -> useCase.processTransaction(SAMPLE_COMMAND)))
                .isInstanceOf(ArielBusinessException.class)
                .satisfies(ex -> {
                    ArielBusinessException biz = (ArielBusinessException) ex;
                    assertThat(biz.getStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
                    assertThat(biz.getErrorCode()).isEqualTo("RISK_REJECTED_SANCTIONS_HIT");
                });

        assertThat(ledgerCallCount.get()).isZero();

        // Verify idempotency lock was released on failure (not stuck in __IN_PROGRESS__)
        var reacquire = idempotencyService.tryAcquire("tenant-acme", "idem-reject-1", Duration.ofSeconds(30));
        assertThat(reacquire).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Acquired.class);
    }

    @Test
    void concurrentInFlightDuplicateThrows409Conflict() {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(5), Duration.ofHours(1));
        var idempotencyService = new RedisIdempotencyService(nearCache, null);

        // Pre-acquire the lock to simulate an in-flight request
        idempotencyService.tryAcquire("tenant-acme", "idem-inflight-99", Duration.ofSeconds(60));

        var useCase = new IngestionOrchestrationUseCase(
                idempotencyService,
                _ -> new RiskDecision.Approved(0.01, "FAST_LANE", 100L),
                cmd -> cmd,
                JsonMapper.builder().build()
        );
        var ctx = new RequestContext("corr-dup", "tenant-acme", "idem-inflight-99", Instant.now());

        assertThatThrownBy(() -> RequestContextHolder.callWith(ctx, () -> useCase.processTransaction(SAMPLE_COMMAND)))
                .isInstanceOf(ArielBusinessException.class)
                .satisfies(ex -> {
                    ArielBusinessException biz = (ArielBusinessException) ex;
                    assertThat(biz.getStatus()).isEqualTo(HttpStatus.CONFLICT);
                    assertThat(biz.getErrorCode()).isEqualTo("DUPLICATE_IN_FLIGHT_REQUEST");
                });
    }
}
