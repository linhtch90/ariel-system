package io.ariel.ingestion.usecase;

import io.ariel.cache.RedisIdempotencyService;
import io.ariel.cache.RedisIdempotencyService.IdempotencyStatus;
import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.RiskEvaluationRequest;
import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.TransactionReceipt;
import io.ariel.common.error.ArielBusinessException;
import io.ariel.ingestion.client.FastPathClientsConfig.LedgerServiceClient;
import io.ariel.ingestion.client.FastPathClientsConfig.RiskEngineClient;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

/**
 * Coordinates the synchronous &lt;15ms P99 Fast Path:
 * 1. Idempotency barrier check (L1 Caffeine + L2 Redis Lua lock)
 * 2. Risk evaluation via @HttpExchange protected by SemaphoreBulkhead + CircuitBreaker
 * 3. Exhaustive Java 25 pattern matching switch over sealed RiskDecision
 * 4. ACID Ledger commit &amp; idempotent response caching
 */
@Service
public class IngestionOrchestrationUseCase {

    private final RedisIdempotencyService idempotencyService;
    private final RiskEngineClient riskEngineClient;
    private final LedgerServiceClient ledgerServiceClient;
    private final JsonMapper jsonMapper;
    private final CircuitBreaker riskCircuitBreaker;
    private final Bulkhead riskBulkhead;

    public IngestionOrchestrationUseCase(
            RedisIdempotencyService idempotencyService,
            RiskEngineClient riskEngineClient,
            LedgerServiceClient ledgerServiceClient,
            JsonMapper jsonMapper
    ) {
        this.idempotencyService = idempotencyService;
        this.riskEngineClient = riskEngineClient;
        this.ledgerServiceClient = ledgerServiceClient;
        this.jsonMapper = jsonMapper;
        this.riskCircuitBreaker = CircuitBreakerRegistry.ofDefaults().circuitBreaker("risk-engine");
        this.riskBulkhead = Bulkhead.of("risk-engine-bulkhead", BulkheadConfig.custom()
                .maxConcurrentCalls(500)
                .maxWaitDuration(Duration.ofMillis(10))
                .build());
    }

    public TransactionReceipt processTransaction(TransactionCommand command) {
        long startNanos = System.nanoTime();
        var ctx = RequestContextHolder.current();

        // 1. Enforce Idempotency-Key deduplication
        IdempotencyStatus idempStatus = idempotencyService.tryAcquire(
                ctx.tenantId(),
                ctx.idempotencyKey(),
                Duration.ofSeconds(60)
        );

        return switch (idempStatus) {
            case IdempotencyStatus.Completed(var cachedJson) -> {
                TransactionReceipt cached = jsonMapper.readValue(cachedJson, TransactionReceipt.class);
                yield new TransactionReceipt(
                        cached.transactionId(),
                        cached.correlationId(),
                        cached.tenantId(),
                        cached.accountId(),
                        cached.amount(),
                        cached.currency(),
                        cached.status(),
                        cached.riskScore(),
                        true,
                        Duration.ofNanos(System.nanoTime() - startNanos).toMillis(),
                        cached.processedAt()
                );
            }
            case IdempotencyStatus.ConcurrentInProgress(_) -> throw new ArielBusinessException(
                    HttpStatus.CONFLICT,
                    "DUPLICATE_IN_FLIGHT_REQUEST",
                    "A request with the same Idempotency-Key is currently in progress: " + ctx.idempotencyKey()
            );
            case IdempotencyStatus.Acquired(var lockKey) -> executeProtectedFastPath(command, ctx, lockKey, startNanos);
        };
    }

    private TransactionReceipt executeProtectedFastPath(
            TransactionCommand command,
            io.ariel.common.context.RequestContext ctx,
            String lockKey,
            long startNanos
    ) {
        try {
            RiskEvaluationRequest riskReq = new RiskEvaluationRequest(
                    ctx.correlationId(),
                    ctx.tenantId(),
                    command.accountId(),
                    command.counterpartyId(),
                    command.amount(),
                    command.currency(),
                    command.region(),
                    command.deviceFingerprint()
            );

            // 2. Call Risk Engine protected by SemaphoreBulkhead + CircuitBreaker
            RiskDecision decision = Bulkhead.decorateSupplier(
                    riskBulkhead,
                    CircuitBreaker.decorateSupplier(riskCircuitBreaker, () -> riskEngineClient.evaluate(riskReq))
            ).get();

            // 3. Exhaustive Java 25 pattern matching over sealed RiskDecision
            TransactionReceipt receipt = switch (decision) {
                case RiskDecision.Approved(var score, _, _) -> {
                    LedgerEntryDto committed = ledgerServiceClient.recordTransaction(new LedgerEntryDto(
                            UUID.randomUUID().toString(),
                            ctx.correlationId(),
                            ctx.tenantId(),
                            command.accountId(),
                            command.counterpartyId(),
                            command.amount(),
                            command.currency(),
                            command.region(),
                            score,
                            BigDecimal.ZERO,
                            Instant.now()
                    ));
                    long elapsedMs = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
                    yield new TransactionReceipt(
                            committed.transactionId(),
                            ctx.correlationId(),
                            ctx.tenantId(),
                            command.accountId(),
                            command.amount(),
                            command.currency(),
                            "APPROVED_AND_COMMITTED",
                            score,
                            false,
                            elapsedMs,
                            committed.committedAt()
                    );
                }
                case RiskDecision.ManualReviewRequired(var score, var queue, _, _) -> {
                    long elapsedMs = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
                    yield new TransactionReceipt(
                            UUID.randomUUID().toString(),
                            ctx.correlationId(),
                            ctx.tenantId(),
                            command.accountId(),
                            command.amount(),
                            command.currency(),
                            "QUEUED_FOR_MANUAL_REVIEW:" + queue,
                            score,
                            false,
                            elapsedMs,
                            Instant.now()
                    );
                }
                case RiskDecision.Rejected(_, var reasonCode, var description, _) -> throw new ArielBusinessException(
                        HttpStatus.UNPROCESSABLE_ENTITY,
                        "RISK_REJECTED_" + reasonCode,
                        description
                );
            };

            // 4. Cache completed receipt for 24h idempotent replay
            idempotencyService.markCompleted(lockKey, jsonMapper.writeValueAsString(receipt));
            return receipt;
        } catch (RuntimeException e) {
            idempotencyService.releaseOnFailure(lockKey);
            throw e;
        }
    }
}
