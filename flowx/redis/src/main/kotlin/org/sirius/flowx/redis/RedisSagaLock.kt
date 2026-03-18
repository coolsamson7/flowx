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

    /**
     * Try-once — returns null immediately if the lock is held by another node.
     * Used by recovery scans where skipping is acceptable.
     */
    override suspend fun <T> withLock(sagaId: String, block: suspend () -> T): T? {
        val key = lockPrefix + sagaId
        val acquired = redis.opsForValue().setIfAbsent(key, ownerId, lockTimeout) ?: false
        if (!acquired) return null

        return try {
            block()
        } finally {
            releaseLock(key)
        }
    }

    /**
     * Waits until the lock is acquired — never skips.
     * Used for event delivery where losing work is not acceptable.
     * Retries indefinitely with [retryDelayMs] between attempts.
     */
    override suspend fun <T> withLockWaiting(sagaId: String, block: suspend () -> T): T {
        val key = lockPrefix + sagaId

        while (true) {
            val acquired = redis.opsForValue().setIfAbsent(key, ownerId, lockTimeout) ?: false
            if (acquired) {
                return try {
                    block()
                } finally {
                    releaseLock(key)
                }
            }
            delay(retryDelayMs)
        }
    }

    private fun releaseLock(key: String) {
        // Only release if we still own it — guards against expired TTL
        // where another node may have re-acquired the lock
        val value = redis.opsForValue().get(key)
        if (value == ownerId) {
            redis.delete(key)
        }
    }
}