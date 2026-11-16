package io.ariel.cache;

import java.time.Duration;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TwoTierNearCacheTest {

    @Test
    void singleFlightCoalescingPreventsCacheStampedeAndL1ServesSubsequentReads() throws Exception {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10));
        var loaderCallCount = new AtomicInteger(0);

        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<String>awaitAllSuccessfulOrThrow(),
                cf -> cf.withName("stampede-test").withTimeout(Duration.ofSeconds(3)))) {

            for (int i = 0; i < 20; i++) {
                scope.fork(() -> nearCache.getOrLoad("tenant:rules:acme", k -> {
                    loaderCallCount.incrementAndGet();
                    try {
                        Thread.sleep(25);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "{\"maxAmount\":50000}";
                }));
            }
            scope.join();
        }

        // Subsequent L1 hit
        String cached = nearCache.getOrLoad("tenant:rules:acme", _ -> "should-not-be-called");
        assertThat(cached).isEqualTo("{\"maxAmount\":50000}");
        assertThat(loaderCallCount.get()).isEqualTo(1);
    }

    @Test
    void idempotencyServiceDeduplicatesCompletedRequests() {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10));
        var idempotency = new RedisIdempotencyService(nearCache, null);

        var first = idempotency.tryAcquire("tenant-1", "key-abc", Duration.ofSeconds(30));
        assertThat(first).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Acquired.class);

        String namespacedKey = ((RedisIdempotencyService.IdempotencyStatus.Acquired) first).namespacedKey();
        idempotency.markCompleted(namespacedKey, "{\"txId\":\"tx-100\"}");

        var second = idempotency.tryAcquire("tenant-1", "key-abc", Duration.ofSeconds(30));
        assertThat(second).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Completed.class);
        assertThat(((RedisIdempotencyService.IdempotencyStatus.Completed) second).cachedJsonPayload())
                .isEqualTo("{\"txId\":\"tx-100\"}");
    }
}
