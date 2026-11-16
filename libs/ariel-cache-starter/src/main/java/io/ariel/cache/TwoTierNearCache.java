package io.ariel.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Industry-standard Two-Tier Near-Cache combining:
 * <ul>
 *   <li><b>L1 Local In-Memory Cache (Caffeine)</b>: &lt;100 microseconds lookup inside each pod</li>
 *   <li><b>L2 Distributed Cache (Redis 8 / Valkey)</b>: &lt;1ms shared state across horizontally scaled pods</li>
 *   <li><b>Single-Flight Coalescing</b>: Prevents cache stampedes (thundering herd) on cold keys</li>
 *   <li><b>Pub/Sub Invalidation</b>: Evicts stale L1 keys across all pods upon writes</li>
 * </ul>
 */
public class TwoTierNearCache {

    private static final Logger log = LoggerFactory.getLogger(TwoTierNearCache.class);
    public static final String INVALIDATION_CHANNEL = "ariel:cache:invalidation";

    private final Cache<String, String> l1Cache;
    private final StringRedisTemplate redisTemplate;
    private final Duration l2Ttl;
    private final ConcurrentHashMap<String, CompletableFuture<String>> inFlightLoads = new ConcurrentHashMap<>();

    public TwoTierNearCache(StringRedisTemplate redisTemplate, long l1MaxSize, Duration l1Ttl, Duration l2Ttl) {
        this.redisTemplate = redisTemplate;
        this.l2Ttl = l2Ttl;
        this.l1Cache = Caffeine.newBuilder()
                .maximumSize(l1MaxSize)
                .expireAfterWrite(l1Ttl)
                .recordStats()
                .build();
    }

    /**
     * Reads from L1 Caffeine -> L2 Redis -> Loader with single-flight stampede protection.
     */
    public String getOrLoad(String key, Function<String, String> loader) {
        // 1. Check L1 Local Caffeine (<100µs)
        String l1Val = l1Cache.getIfPresent(key);
        if (l1Val != null) {
            return l1Val;
        }

        // 2. Single-flight coalescing to prevent cache stampede on L1 miss
        CompletableFuture<String> future = inFlightLoads.computeIfAbsent(key, k ->
                CompletableFuture.supplyAsync(() -> loadThroughL2(k, loader))
        );

        try {
            return future.join();
        } finally {
            inFlightLoads.remove(key, future);
        }
    }

    private String loadThroughL2(String key, Function<String, String> loader) {
        // Check L2 Redis (<1ms)
        if (redisTemplate != null) {
            try {
                String l2Val = redisTemplate.opsForValue().get(key);
                if (l2Val != null) {
                    l1Cache.put(key, l2Val);
                    return l2Val;
                }
            } catch (Exception e) {
                log.warn("L2 Redis read degraded for key={}, falling back to source loader: {}", key, e.getMessage());
            }
        }

        // Load from authoritative source
        String loaded = loader.apply(key);
        if (loaded != null) {
            put(key, loaded);
        }
        return loaded;
    }

    public Optional<String> getIfPresent(String key) {
        String l1Val = l1Cache.getIfPresent(key);
        if (l1Val != null) {
            return Optional.of(l1Val);
        }
        if (redisTemplate != null) {
            try {
                String l2Val = redisTemplate.opsForValue().get(key);
                if (l2Val != null) {
                    l1Cache.put(key, l2Val);
                    return Optional.of(l2Val);
                }
            } catch (Exception e) {
                log.warn("L2 Redis read failed for key={}: {}", key, e.getMessage());
            }
        }
        return Optional.empty();
    }

    public void put(String key, String value) {
        l1Cache.put(key, value);
        if (redisTemplate != null) {
            try {
                redisTemplate.opsForValue().set(key, value, l2Ttl);
            } catch (Exception e) {
                log.warn("L2 Redis write failed for key={}: {}", key, e.getMessage());
            }
        }
    }

    /**
     * Evicts from L2 Redis and broadcasts an invalidation notice so all horizontally scaled pods
     * immediately evict the key from their local L1 Caffeine cache.
     */
    public void invalidateClusterWide(String key) {
        l1Cache.invalidate(key);
        if (redisTemplate != null) {
            try {
                redisTemplate.delete(key);
                redisTemplate.convertAndSend(INVALIDATION_CHANNEL, key);
            } catch (Exception e) {
                log.warn("Failed to broadcast cache invalidation for key={}: {}", key, e.getMessage());
            }
        }
    }

    /**
     * Invoked when a Redis Pub/Sub message arrives on {@link #INVALIDATION_CHANNEL}.
     */
    public void onInvalidationMessage(String key) {
        l1Cache.invalidate(key);
    }

    public CacheStats stats() {
        return l1Cache.stats();
    }
}
