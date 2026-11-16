package io.ariel.cache;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TwoTierNearCacheTest {

    @Test
    void singleFlightCoalescingPreventsCacheStampedeAndL1ServesSubsequentReads() throws Exception {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10));
        var loaderCallCount = new AtomicInteger(0);

        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<String>awaitAllSuccessfulOrThrow(),
                cf -> cf.withName("stampede-test").withTimeout(Duration.ofSeconds(3)))) {

            for (int i = 0; i < 20; i++) {
                scope.fork(() -> nearCache.getOrLoad("tenant:rules:acme", _ -> {
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
        assertThat(nearCache.stats().hitCount()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    void promotesL2RedisHitIntoL1CacheWithoutCallingDatabaseLoaderAndBroadcastsInvalidations() {
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> valueOps = Mockito.mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get("key-in-l2")).thenReturn("value-from-redis-l2");

        var nearCache = new TwoTierNearCache(redisTemplate, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10));
        var dbLoaderCalled = new AtomicInteger(0);

        // 1. L1 miss -> L2 Redis hit -> populates L1 without calling DB loader
        String val = nearCache.getOrLoad("key-in-l2", _ -> {
            dbLoaderCalled.incrementAndGet();
            return "db-value";
        });
        assertThat(val).isEqualTo("value-from-redis-l2");
        assertThat(dbLoaderCalled.get()).isZero();

        // 2. Cluster-wide invalidation deletes from L2 and broadcasts on Pub/Sub channel
        nearCache.invalidateClusterWide("key-in-l2");
        verify(redisTemplate).delete("key-in-l2");
        verify(redisTemplate).convertAndSend(TwoTierNearCache.INVALIDATION_CHANNEL, "key-in-l2");
    }

    @Test
    void idempotencyServiceHandlesInProgressConflictCompletedReplayAndReleaseOnFailure() {
        var nearCache = new TwoTierNearCache(null, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10));
        var idempotency = new RedisIdempotencyService(nearCache, null);

        // 1. First caller acquires lock
        var first = idempotency.tryAcquire("tenant-1", "key-abc", Duration.ofSeconds(30));
        assertThat(first).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Acquired.class);
        String namespacedKey = ((RedisIdempotencyService.IdempotencyStatus.Acquired) first).namespacedKey();

        // 2. Concurrent caller arriving while first is still in progress gets ConcurrentInProgress
        var concurrent = idempotency.tryAcquire("tenant-1", "key-abc", Duration.ofSeconds(30));
        assertThat(concurrent).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.ConcurrentInProgress.class);

        // 3. Simulate downstream failure -> releaseOnFailure frees lock so client can retry
        idempotency.releaseOnFailure(namespacedKey);

        var retryAfterFailure = idempotency.tryAcquire("tenant-1", "key-abc", Duration.ofSeconds(30));
        assertThat(retryAfterFailure).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Acquired.class);

        // 4. Mark completed -> subsequent retries receive Completed payload
        idempotency.markCompleted(namespacedKey, "{\"txId\":\"tx-100\"}");
        var completed = idempotency.tryAcquire("tenant-1", "key-abc", Duration.ofSeconds(30));
        assertThat(completed).isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Completed.class);
        assertThat(((RedisIdempotencyService.IdempotencyStatus.Completed) completed).cachedJsonPayload())
                .isEqualTo("{\"txId\":\"tx-100\"}");
    }

    @Test
    void idempotencyServiceExecutesAtomicRedisLuaScriptWhenRedisIsAvailable() {
        StringRedisTemplate redisTemplate = Mockito.mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> valueOps = Mockito.mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(any())).thenReturn(null);
        when(redisTemplate.execute(any(), eq(List.of("ariel:idemp:t1:k1")), eq("__IN_PROGRESS__"), any()))
                .thenReturn("ACQUIRED")
                .thenReturn("__IN_PROGRESS__");

        var nearCache = new TwoTierNearCache(redisTemplate, 1000, Duration.ofMinutes(1), Duration.ofMinutes(10));
        var idempotency = new RedisIdempotencyService(nearCache, redisTemplate);

        assertThat(idempotency.tryAcquire("t1", "k1", Duration.ofSeconds(60)))
                .isInstanceOf(RedisIdempotencyService.IdempotencyStatus.Acquired.class);
        assertThat(idempotency.tryAcquire("t1", "k1", Duration.ofSeconds(60)))
                .isInstanceOf(RedisIdempotencyService.IdempotencyStatus.ConcurrentInProgress.class);
        verify(redisTemplate, never()).delete(any(String.class));
    }
}
