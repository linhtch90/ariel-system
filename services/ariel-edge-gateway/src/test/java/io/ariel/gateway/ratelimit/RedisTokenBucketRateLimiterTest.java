package io.ariel.gateway.ratelimit;

import io.ariel.common.error.ArielBusinessException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisTokenBucketRateLimiterTest {

    @Test
    void enforcesTokenBucketRateLimitAndThrowsProblemDetailExceptionWhenExhausted() {
        var emptyProvider = new StaticListableBeanFactory().getBeanProvider(StringRedisTemplate.class);
        var limiter = new RedisTokenBucketRateLimiter(emptyProvider, 3);

        assertThat(limiter.checkAndConsumeToken("tenant-alpha")).isEqualTo(2);
        assertThat(limiter.checkAndConsumeToken("tenant-alpha")).isEqualTo(1);
        assertThat(limiter.checkAndConsumeToken("tenant-alpha")).isZero();

        assertThatThrownBy(() -> limiter.checkAndConsumeToken("tenant-alpha"))
                .isInstanceOf(ArielBusinessException.class)
                .hasMessageContaining("tenant-alpha");
    }
}
