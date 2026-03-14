package org.sirius.flowx.redis

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate

@Configuration
class RedisFlowxConfiguration(private val redis: StringRedisTemplate) {
    @Bean
    fun sagaLock() = RedisSagaLock(redis)

    @Bean
    fun timeoutQueue() = RedisTimeoutQueue(redis)
}