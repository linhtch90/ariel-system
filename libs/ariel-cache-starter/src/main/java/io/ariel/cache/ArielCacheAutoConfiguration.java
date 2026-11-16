package io.ariel.cache;

import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

@AutoConfiguration
public class ArielCacheAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public TwoTierNearCache twoTierNearCache(ObjectProvider<StringRedisTemplate> redisTemplateProvider) {
        return new TwoTierNearCache(
                redisTemplateProvider.getIfAvailable(),
                50_000L,
                Duration.ofSeconds(30),
                Duration.ofHours(24)
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public RedisIdempotencyService redisIdempotencyService(
            TwoTierNearCache twoTierNearCache,
            ObjectProvider<StringRedisTemplate> redisTemplateProvider
    ) {
        return new RedisIdempotencyService(twoTierNearCache, redisTemplateProvider.getIfAvailable());
    }
}
