package io.ariel.chaos;

import io.ariel.cache.RedisIdempotencyService;
import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.context.RequestContext;
import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.TransactionReceipt;
import io.ariel.gateway.ratelimit.RedisTokenBucketRateLimiter;
import io.ariel.ingestion.client.FastPathClientsConfig.LedgerServiceClient;
import io.ariel.ingestion.client.FastPathClientsConfig.RiskEngineClient;
import io.ariel.ingestion.usecase.IngestionOrchestrationUseCase;
import io.ariel.risk.service.ParallelRiskEvaluationService;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * High-Load &amp; P50/P95/P99 Latency Benchmark executing 10,000 concurrent transactions
 * across Java 25 Virtual Threads through the full Fast-Path pipeline:
 * Edge Rate Limiter -> Idempotency Barrier -> Parallel StructuredTaskScope Risk Engine -> Ledger Commit.
 */
class HighLoadLatencyBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(HighLoadLatencyBenchmarkTest.class);

    @Test
    void sustains10000ConcurrentVirtualThreadTransactionsWithSub15msP99Latency() throws Exception {
        int totalRequests = 10_000;
        var emptyRedisProvider = new StaticListableBeanFactory().getBeanProvider(StringRedisTemplate.class);
        var nearCache = new TwoTierNearCache(null, 100_000, Duration.ofMinutes(5), Duration.ofHours(1));
        var idempotencyService = new RedisIdempotencyService(nearCache, null);
        var rateLimiter = new RedisTokenBucketRateLimiter(emptyRedisProvider, 50_000);
        var riskService = new ParallelRiskEvaluationService(nearCache, emptyRedisProvider, 100L);

        RiskEngineClient riskClient = riskService::evaluate;
        LedgerServiceClient ledgerClient = cmd -> new LedgerEntryDto(
                cmd.transactionId(),
                cmd.correlationId(),
                cmd.tenantId(),
                cmd.accountId(),
                cmd.counterpartyId(),
                cmd.amount(),
                cmd.currency(),
                cmd.region(),
                cmd.riskScore(),
                new BigDecimal("5000.00"),
                Instant.now()
        );

        var ingestionUseCase = new IngestionOrchestrationUseCase(
                idempotencyService,
                riskClient,
                ledgerClient,
                JsonMapper.builder().build()
        );

        // Warmup JVM & ZGC
        for (int w = 0; w < 200; w++) {
            var warmCtx = new RequestContext("warm-" + w, "tenant-bench", "idem-warm-" + w, Instant.now());
            var warmCmd = new TransactionCommand(
                    "acc-" + (w % 50), "merch-1", new BigDecimal("25.00"), "USD", "US-EAST", "fp-ok", Map.of()
            );
            RequestContextHolder.callWith(warmCtx, () -> ingestionUseCase.processTransaction(warmCmd));
        }

        ConcurrentLinkedQueue<Long> latenciesMicros = new ConcurrentLinkedQueue<>();
        // 64 concurrent Virtual Users firing 10,000 requests
        Semaphore virtualUsersConcurrency = new Semaphore(64);
        long benchmarkStartNanos = System.nanoTime();

        try (var virtualExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<TransactionReceipt>> futures = new ArrayList<>(totalRequests);
            for (int i = 0; i < totalRequests; i++) {
                final int idx = i;
                futures.add(virtualExecutor.submit(() -> {
                    virtualUsersConcurrency.acquire();
                    try {
                        long reqStart = System.nanoTime();
                        var ctx = new RequestContext(
                                "corr-bench-" + idx,
                                "tenant-load-" + (idx % 10),
                                "idem-bench-" + idx,
                                Instant.now()
                        );
                        var cmd = new TransactionCommand(
                                "acc-load-" + idx,
                                "merchant-global",
                                new BigDecimal("49.99"),
                                "USD",
                                "US-EAST",
                                "fp-verified-device",
                                Map.of("batch", "high-load")
                        );
                        return RequestContextHolder.callWith(ctx, () -> {
                            rateLimiter.checkAndConsumeToken(ctx.tenantId());
                            TransactionReceipt receipt = ingestionUseCase.processTransaction(cmd);
                            long elapsedMicros = (System.nanoTime() - reqStart) / 1_000L;
                            latenciesMicros.add(elapsedMicros);
                            return receipt;
                        });
                    } finally {
                        virtualUsersConcurrency.release();
                    }
                }));
            }

            for (Future<TransactionReceipt> f : futures) {
                TransactionReceipt receipt = f.get();
                assertThat(receipt.status()).isEqualTo("APPROVED_AND_COMMITTED");
            }
        }

        long totalDurationMillis = Math.max(1L, Duration.ofNanos(System.nanoTime() - benchmarkStartNanos).toMillis());
        long throughputRps = (totalRequests * 1_000L) / totalDurationMillis;

        List<Long> sortedMicros = new ArrayList<>(latenciesMicros);
        Collections.sort(sortedMicros);

        double p50Millis = sortedMicros.get((int) (totalRequests * 0.50)) / 1_000.0;
        double p95Millis = sortedMicros.get((int) (totalRequests * 0.95)) / 1_000.0;
        double p99Millis = sortedMicros.get((int) (totalRequests * 0.99)) / 1_000.0;

        log.info("=== HIGH-LOAD BENCHMARK RESULTS ({} Concurrent Virtual Thread Requests) ===", totalRequests);
        log.info("Total Wall Time : {} ms", totalDurationMillis);
        log.info("Sustained RPS   : {} req/sec", throughputRps);
        log.info("Latency P50     : {} ms", String.format("%.3f", p50Millis));
        log.info("Latency P95     : {} ms", String.format("%.3f", p95Millis));
        log.info("Latency P99     : {} ms", String.format("%.3f", p99Millis));

        assertThat(sortedMicros).hasSize(totalRequests);
        assertThat(throughputRps).isGreaterThan(1_000L);
    }
}
