package com.pulsestream.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Configures the Redis client (Spring Data Redis / Lettuce).
 *
 * <p>Redis serves two purposes in PulseStream:
 * <ol>
 *   <li>Idempotency key cache — fast O(1) check before the PostgreSQL UNIQUE constraint.
 *   <li>Rate limiting — sliding window counter per client IP / API key.
 * </ol>
 *
 * <p>The application degrades gracefully if Redis is unavailable:
 * idempotency falls back to the PostgreSQL UNIQUE constraint (correct but slower),
 * and rate limiting is bypassed.
 *
 * <p>Connection details are configured in application.yml via environment variables
 * (REDIS_HOST, REDIS_PORT, REDIS_PASSWORD).
 */
@Configuration
public class RedisConfig {

    /**
     * String-keyed, String-valued RedisTemplate for idempotency keys.
     * Using String serializers keeps keys human-readable and avoids Java
     * serialization issues across application restarts.
     */
    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }
}
