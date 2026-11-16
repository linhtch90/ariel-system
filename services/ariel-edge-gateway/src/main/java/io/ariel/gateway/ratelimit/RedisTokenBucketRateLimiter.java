package io.ariel.gateway.ratelimit;

import io.ariel.common.error.ArielBusinessException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Distributed Token-Bucket Rate Limiter powered by an atomic Redis Lua script
 * (with resilient in-memory per-pod token bucket fallback if Redis is unreachable).
 */
@Component
public class RedisTokenBucketRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisTokenBucketRateLimiter.class);

    private static final DefaultRedisScript<Long> TOKEN_BUCKET_LUA = new DefaultRedisScript<>(
            """
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local window_sec = tonumber(ARGV[2])
            local current = tonumber(redis.call('GET', key) or "0")
            if current + 1 > capacity then
                return -1
            end
            current = redis.call('INCR', key)
            if current == 1 then
                redis.call('EXPIRE', key, window_sec)
            end
            return capacity - current
            """,
            Long.class
    );

    private final StringRedisTemplate redisTemplate;
    private final int burstCapacity;
    private final ConcurrentHashMap<String, AtomicInteger> localBuckets = new ConcurrentHashMap<>();

    public RedisTokenBucketRateLimiter(
            ObjectProvider<StringRedisTemplate> redisTemplateProvider,
            @Value("${ariel.gateway.rate-limit.burst-capacity:5000}") int burstCapacity
    ) {
        this.redisTemplate = redisTemplateProvider.getIfAvailable();
        this.burstCapacity = burstCapacity;
    }

    public long checkAndConsumeToken(String tenantId) {
        long epochSecondWindow = Instant.now().getEpochSecond();
        String bucketKey = "ariel:ratelimit:" + tenantId + ":" + epochSecondWindow;

        if (redisTemplate != null) {
            try {
                Long remaining = redisTemplate.execute(
                        TOKEN_BUCKET_LUA,
                        List.of(bucketKey),
                        String.valueOf(burstCapacity),
                        "2"
                );
                if (remaining != null) {
                    if (remaining < 0) {
                        throw new ArielBusinessException(
                                HttpStatus.TOO_MANY_REQUESTS,
                                "TENANT_RATE_LIMIT_EXCEEDED",
                                "Rate limit exceeded for tenant: " + tenantId
                        );
                    }
                    return remaining;
                }
            } catch (ArielBusinessException e) {
                throw e;
            } catch (Exception e) {
                log.debug("Redis rate-limiter fallback triggered for tenant={}: {}", tenantId, e.getMessage());
            }
        }

        AtomicInteger counter = localBuckets.computeIfAbsent(bucketKey, _ -> new AtomicInteger(0));
        int used = counter.incrementAndGet();
        if (used > burstCapacity) {
            throw new ArielBusinessException(
                    HttpStatus.TOO_MANY_REQUESTS,
                    "TENANT_RATE_LIMIT_EXCEEDED",
                    "Rate limit exceeded for tenant: " + tenantId
            );
        }
        return burstCapacity - used;
    }
}
