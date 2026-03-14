package org.sirius.flowx.redis
/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import org.sirius.flowx.SagaLock
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration
import java.util.UUID
import kotlinx.coroutines.delay


class RedisSagaLock(
    private val redis: StringRedisTemplate,
    private val lockTimeout: Duration = Duration.ofSeconds(10),
    private val retryDelayMs: Long = 50,
    private val ownerId: String = UUID.randomUUID().toString()
) : SagaLock {

    private val lockPrefix = "saga-lock:"

    override suspend fun <T> withLock(sagaId: String, block: suspend () -> T): T? {
        val key = lockPrefix + sagaId
        val timeoutMillis = lockTimeout.toMillis()
        val startTime = System.currentTimeMillis()

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            val acquired = redis.opsForValue().setIfAbsent(key, ownerId, lockTimeout)
                ?: false
            if (acquired) {
                try {
                    return block()
                } finally {
                    val value = redis.opsForValue().get(key)
                    if (value == ownerId) {
                        redis.delete(key)
                    }
                }
            }
            delay(retryDelayMs) // coroutine-friendly retry
        }

        return null // timeout acquiring lock
    }
}