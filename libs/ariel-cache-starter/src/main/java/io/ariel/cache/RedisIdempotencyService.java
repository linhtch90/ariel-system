package io.ariel.cache;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Enforces strict Idempotency-Key deduplication at the Edge/Ingestion boundary using an atomic
 * Redis Lua script backed by the Two-Tier Near-Cache.
 */
public class RedisIdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyService.class);
    private static final String IN_PROGRESS_MARKER = "__IN_PROGRESS__";

    private static final DefaultRedisScript<String> ACQUIRE_LOCK_SCRIPT = new DefaultRedisScript<>(
            """
            local current = redis.call('GET', KEYS[1])
            if current then
                return current
            end
            redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            return 'ACQUIRED'
            """,
            String.class
    );

    private final TwoTierNearCache nearCache;
    private final StringRedisTemplate redisTemplate;
    private final ConcurrentHashMap<String, String> localFallbackLocks = new ConcurrentHashMap<>();

    public RedisIdempotencyService(TwoTierNearCache nearCache, StringRedisTemplate redisTemplate) {
        this.nearCache = nearCache;
        this.redisTemplate = redisTemplate;
    }

    public sealed interface IdempotencyStatus {
        record Acquired(String namespacedKey) implements IdempotencyStatus {}
        record Completed(String cachedJsonPayload) implements IdempotencyStatus {}
        record ConcurrentInProgress(String namespacedKey) implements IdempotencyStatus {}
    }

    public IdempotencyStatus tryAcquire(String tenantId, String idempotencyKey, Duration lockTimeout) {
        String key = "ariel:idemp:" + tenantId + ":" + idempotencyKey;

        // 1. Fast check in L1/L2 Near-Cache for already completed response
        Optional<String> existing = nearCache.getIfPresent(key);
        if (existing.isPresent() && !IN_PROGRESS_MARKER.equals(existing.get())) {
            return new IdempotencyStatus.Completed(existing.get());
        }

        // 2. Atomic Redis Lua lock acquisition
        if (redisTemplate != null) {
            try {
                String result = redisTemplate.execute(
                        ACQUIRE_LOCK_SCRIPT,
                        List.of(key),
                        IN_PROGRESS_MARKER,
                        String.valueOf(lockTimeout.toMillis())
                );
                if ("ACQUIRED".equals(result)) {
                    return new IdempotencyStatus.Acquired(key);
                }
                if (IN_PROGRESS_MARKER.equals(result)) {
                    return new IdempotencyStatus.ConcurrentInProgress(key);
                }
                if (result != null) {
                    nearCache.put(key, result);
                    return new IdempotencyStatus.Completed(result);
                }
            } catch (Exception e) {
                log.warn("Redis idempotency check degraded, using local lock fallback for key={}: {}", key, e.getMessage());
            }
        }

        // 3. Resilient local fallback if Redis is unreachable in local/dev mode
        String prev = localFallbackLocks.putIfAbsent(key, IN_PROGRESS_MARKER);
        if (prev == null) {
            return new IdempotencyStatus.Acquired(key);
        }
        if (IN_PROGRESS_MARKER.equals(prev)) {
            return new IdempotencyStatus.ConcurrentInProgress(key);
        }
        return new IdempotencyStatus.Completed(prev);
    }

    public void markCompleted(String namespacedKey, String jsonPayload) {
        localFallbackLocks.put(namespacedKey, jsonPayload);
        nearCache.put(namespacedKey, jsonPayload);
    }

    public void releaseOnFailure(String namespacedKey) {
        localFallbackLocks.remove(namespacedKey);
        nearCache.invalidateClusterWide(namespacedKey);
    }
}
