package io.ariel.chaos;

import io.ariel.common.error.ArielBusinessException;
import io.ariel.gateway.ratelimit.RedisTokenBucketRateLimiter;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies system behavior when reaching hard capacity limits:
 * <ol>
 *   <li>Noisy-Neighbor Multi-Tenant Rate Limiting: Exhausting one tenant's burst quota sheds load
 *       with HTTP 429 while leaving VIP tenants 100% unaffected.</li>
 *   <li>SemaphoreBulkhead Concurrency Saturation: When a downstream service slows down under extreme concurrency,
 *       the SemaphoreBulkhead rejects excess virtual threads immediately rather than exhausting JVM memory.</li>
 * </ol>
 */
class SystemLimitsAndBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(SystemLimitsAndBackpressureTest.class);

    @Test
    void noisyNeighborTenantIsThrottledAtBurstLimitWithZeroImpactOnOtherTenants() {
        var emptyRedisProvider = new StaticListableBeanFactory().getBeanProvider(StringRedisTemplate.class);
        int burstCapacity = 100;
        var rateLimiter = new RedisTokenBucketRateLimiter(emptyRedisProvider, burstCapacity);

        int allowedNoisy = 0;
        int rejectedNoisy429 = 0;

        // Fire 250 requests from "tenant-noisy-attacker" (limit = 100)
        for (int i = 0; i < 250; i++) {
            try {
                rateLimiter.checkAndConsumeToken("tenant-noisy-attacker");
                allowedNoisy++;
            } catch (ArielBusinessException ex) {
                assertThat(ex.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
                assertThat(ex.getErrorCode()).isEqualTo("TENANT_RATE_LIMIT_EXCEEDED");
                rejectedNoisy429++;
            }
        }

        // Meanwhile, "tenant-vip-enterprise" sends 100 requests and experiences 0% packet loss
        int allowedVip = 0;
        for (int i = 0; i < 100; i++) {
            rateLimiter.checkAndConsumeToken("tenant-vip-enterprise");
            allowedVip++;
        }

        log.info("Noisy Tenant: allowed={}, rejected429={} | VIP Tenant: allowed={}/100",
                allowedNoisy, rejectedNoisy429, allowedVip);

        assertThat(allowedNoisy).isEqualTo(100);
        assertThat(rejectedNoisy429).isEqualTo(150);
        assertThat(allowedVip).isEqualTo(100);
    }

    @Test
    void semaphoreBulkheadShedsExcessLoadWhenDownstreamReachesConcurrencyLimit() throws Exception {
        int maxConcurrentPermits = 10;
        int totalSpikeCalls = 50;

        Bulkhead bulkhead = Bulkhead.of("risk-saturation-test", BulkheadConfig.custom()
                .maxConcurrentCalls(maxConcurrentPermits)
                .maxWaitDuration(Duration.ZERO)
                .build());

        CountDownLatch holdPermitsLatch = new CountDownLatch(1);
        CountDownLatch permitsAcquiredLatch = new CountDownLatch(maxConcurrentPermits);
        AtomicInteger succeededCalls = new AtomicInteger(0);
        AtomicInteger rejectedByBulkhead = new AtomicInteger(0);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(totalSpikeCalls);

            // 1. First 10 virtual threads acquire all 10 bulkhead permits and hold them
            for (int i = 0; i < maxConcurrentPermits; i++) {
                futures.add(executor.submit(() -> {
                    Bulkhead.decorateRunnable(bulkhead, () -> {
                        permitsAcquiredLatch.countDown();
                        try {
                            holdPermitsLatch.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        succeededCalls.incrementAndGet();
                    }).run();
                }));
            }

            // Wait until all 10 permits are actively held
            permitsAcquiredLatch.await();

            // 2. Next 40 concurrent virtual threads arrive while bulkhead is 100% saturated
            List<Future<?>> excessFutures = new ArrayList<>(totalSpikeCalls - maxConcurrentPermits);
            for (int i = 0; i < totalSpikeCalls - maxConcurrentPermits; i++) {
                excessFutures.add(executor.submit(() -> {
                    try {
                        Bulkhead.decorateRunnable(bulkhead, succeededCalls::incrementAndGet).run();
                    } catch (BulkheadFullException _) {
                        rejectedByBulkhead.incrementAndGet();
                    }
                }));
            }
            for (Future<?> ef : excessFutures) {
                ef.get();
            }

            // Now release the 10 held permits
            holdPermitsLatch.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        log.info("Bulkhead Limit Test: maxPermits={}, succeeded={}, rejectedByBulkhead={}",
                maxConcurrentPermits, succeededCalls.get(), rejectedByBulkhead.get());

        assertThat(succeededCalls.get()).isEqualTo(maxConcurrentPermits);
        assertThat(rejectedByBulkhead.get()).isEqualTo(totalSpikeCalls - maxConcurrentPermits);
    }
}
