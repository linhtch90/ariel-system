package io.ariel.chaos;

import io.ariel.cache.RedisIdempotencyService;
import io.ariel.cache.TwoTierNearCache;
import io.ariel.common.context.RequestContext;
import io.ariel.common.context.RequestContextHolder;
import io.ariel.common.domain.LedgerEntryDto;
import io.ariel.common.domain.RiskDecision;
import io.ariel.common.domain.RiskEvaluationRequest;
import io.ariel.common.domain.TransactionCommand;
import io.ariel.common.domain.ValidatedTransactionEvent;
import io.ariel.ingestion.client.FastPathClientsConfig.LedgerServiceClient;
import io.ariel.ingestion.client.FastPathClientsConfig.RiskEngineClient;
import io.ariel.ingestion.usecase.IngestionOrchestrationUseCase;
import io.ariel.risk.service.ParallelRiskEvaluationService;
import io.ariel.stream.processor.ValidatedTransactionStreamProcessor;
import io.ariel.stream.sink.PolyglotAnalyticsSink;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Automated Chaos Engineering &amp; Fault-Injection Suite validating system resilience under failure:
 * <ol>
 *   <li><b>Chaos 1</b>: Downstream Risk Engine outage trips Resilience4j CircuitBreaker to fail fast,
 *       preventing cascading thread/socket exhaustion.</li>
 *   <li><b>Chaos 2</b>: Slow downstream dependency hang (500ms) is automatically interrupted by
 *       Java 25 {@code StructuredTaskScope} timeout (25ms) and degrades gracefully to {@code ManualReviewRequired}.</li>
 *   <li><b>Chaos 3</b>: Total Redis L2 Cluster blackout mid-flight degrades gracefully to L1 Caffeine / local
 *       locks with zero unhandled exceptions.</li>
 *   <li><b>Chaos 4</b>: 5,000-event Kafka flood injected with corrupted poison-pill payloads processes all
 *       valid events via {@code Gatherers.windowFixed(100)} while isolating 100% of poison pills into {@code ariel.dlq}.</li>
 * </ol>
 */
class ChaosFaultInjectionResilienceTest {

    private static final Logger log = LoggerFactory.getLogger(ChaosFaultInjectionResilienceTest.class);

    @Test
    void chaos1_downstreamOutageTripsCircuitBreakerOpenToFailFast() {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(1), Duration.ofHours(1));
        var idempotencyService = new RedisIdempotencyService(nearCache, null);
        AtomicBoolean downstreamOutage = new AtomicBoolean(true);

        RiskEngineClient chaoticRiskClient = _ -> {
            if (downstreamOutage.get()) {
                throw new RuntimeException("CHAOS_INJECTED: Risk Engine Connection Refused");
            }
            return new RiskDecision.Approved(0.02, "FAST_LANE", 500L);
        };

        LedgerServiceClient ledgerClient = cmd -> cmd;
        var useCase = new IngestionOrchestrationUseCase(
                idempotencyService,
                chaoticRiskClient,
                ledgerClient,
                JsonMapper.builder().build()
        );

        var cmd = new TransactionCommand(
                "acc-chaos-1", "merch-1", new BigDecimal("100.00"), "USD", "US-EAST", "fp-1", Map.of()
        );

        // Send 100 failing calls to fill the CircuitBreaker sliding window (100 calls) and trip it OPEN
        for (int i = 0; i < 100; i++) {
            final int idx = i;
            var ctx = new RequestContext("corr-cb-" + idx, "tenant-chaos", "idem-cb-" + idx, Instant.now());
            assertThatThrownBy(() -> RequestContextHolder.callWith(ctx, () -> useCase.processTransaction(cmd)))
                    .isInstanceOf(RuntimeException.class);
        }

        // Call #101 must fail fast with CallNotPermittedException (CircuitBreaker is now OPEN!)
        var openCtx = new RequestContext("corr-cb-open", "tenant-chaos", "idem-cb-open", Instant.now());
        assertThatThrownBy(() -> RequestContextHolder.callWith(openCtx, () -> useCase.processTransaction(cmd)))
                .isInstanceOf(CallNotPermittedException.class);

        log.info("Chaos 1 Verified: CircuitBreaker tripped OPEN and failed fast with CallNotPermittedException");
    }

    @Test
    void chaos2_slowDependencyLatencySpikeIsCancelledByStructuredTaskScopeDeadline() {
        // Inject a slow L2 loader that sleeps for 400ms while Risk Engine deadline is strictly 25ms
        TwoTierNearCache slowCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(1), Duration.ofMinutes(5)) {
            @Override
            public String getOrLoad(String key, java.util.function.Function<String, String> loader) {
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "STANDARD_TIER";
            }
        };

        var emptyRedisProvider = new StaticListableBeanFactory().getBeanProvider(StringRedisTemplate.class);
        var riskService = new ParallelRiskEvaluationService(slowCache, emptyRedisProvider, 25L);

        var req = new RiskEvaluationRequest(
                "corr-slow", "tenant-slow", "acc-slow", "merch-1",
                new BigDecimal("150.00"), "USD", "US-EAST", "fp-ok"
        );

        long startMs = System.currentTimeMillis();
        RiskDecision decision = riskService.evaluate(req);
        long elapsedMs = System.currentTimeMillis() - startMs;

        log.info("Chaos 2 Verified: 400ms hang interrupted in {}ms -> degraded to {}", elapsedMs, decision);

        assertThat(elapsedMs).isLessThan(250L);
        assertThat(decision).isInstanceOf(RiskDecision.ManualReviewRequired.class);
    }

    @Test
    void chaos3_totalRedisClusterOutageDegradesGracefullyToL1CaffeineAndLocalLocks() {
        // Create a broken StringRedisTemplate that throws RedisConnectionFailureException on every operation
        StringRedisTemplate brokenRedis = Mockito.mock(StringRedisTemplate.class);
        Mockito.when(brokenRedis.opsForValue()).thenThrow(new RedisConnectionFailureException("CHAOS: Redis Cluster Down"));
        Mockito.when(brokenRedis.execute(Mockito.any(), Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenThrow(new RedisConnectionFailureException("CHAOS: Redis Cluster Down"));

        var nearCache = new TwoTierNearCache(brokenRedis, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10));
        var idempotencyService = new RedisIdempotencyService(nearCache, brokenRedis);

        // 1. Near-Cache falls back to source loader + L1 Caffeine despite Redis crash
        String val = nearCache.getOrLoad("tenant:rule:chaos", _ -> "FALLBACK_RULE_OK");
        assertThat(val).isEqualTo("FALLBACK_RULE_OK");

        // 2. Idempotency falls back to local lock & L1 cache despite Redis crash
        var status1 = idempotencyService.tryAcquire("tenant-1", "idem-redis-down", Duration.ofSeconds(30));
        assertThat(status1).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Acquired.class);

        String lockKey = ((RedisIdempotencyService.IdempotencyStatus.Acquired) status1).namespacedKey();
        idempotencyService.markCompleted(lockKey, "{\"status\":\"SURVIVED_REDIS_OUTAGE\"}");

        var status2 = idempotencyService.tryAcquire("tenant-1", "idem-redis-down", Duration.ofSeconds(30));
        assertThat(status2).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Completed.class);

        log.info("Chaos 3 Verified: System survived total Redis L2 blackout with zero downtime");
    }

    @Test
    void chaos4_streamFloodWithPoisonPillsIsolatesCorruptedRecordsToDlqWithoutStoppingValidBatches() {
        var nearCache = new TwoTierNearCache(null, 10_000, Duration.ofMinutes(5), Duration.ofMinutes(30));
        var sink = new PolyglotAnalyticsSink(nearCache);
        var mapper = JsonMapper.builder().build();
        @SuppressWarnings("unchecked")
        var kafkaProvider = (org.springframework.beans.factory.ObjectProvider<KafkaTemplate<String, String>>)
                (org.springframework.beans.factory.ObjectProvider<?>)
                        new StaticListableBeanFactory().getBeanProvider(KafkaTemplate.class);

        var processor = new ValidatedTransactionStreamProcessor(sink, mapper, kafkaProvider, 100);

        int validCount = 5_000;
        int poisonPillCount = 25;
        List<String> floodBatch = new ArrayList<>(validCount + poisonPillCount);

        for (int i = 1; i <= validCount; i++) {
            var ev = new ValidatedTransactionEvent(
                    "ev-" + i, "tx-" + i, "corr-" + i, "tenant-flood",
                    "acc-" + (i % 100), "merch-1", new BigDecimal("19.99"), "USD", "US-EAST",
                    0.02, new BigDecimal("1000.00"), Instant.parse("2026-11-16T12:00:00Z")
            );
            floodBatch.add(mapper.writeValueAsString(ev));
            if (i % 200 == 0) {
                floodBatch.add("{\"corrupted_poison_pill\": true, \"transactionId\": null}");
            }
        }

        int processed = processor.processRawBatch(floodBatch);

        log.info("Chaos 4 Verified: Processed {}/{} valid stream events across micro-batches; quarantined {} poison pills to ariel.dlq",
                processed, validCount, processor.getDlqBuffer().size());

        assertThat(processed).isEqualTo(validCount);
        assertThat(sink.getClickHouseCountForTenant("tenant-flood")).isEqualTo(validCount);
        assertThat(processor.getDlqBuffer()).hasSize(poisonPillCount);
    }
}
