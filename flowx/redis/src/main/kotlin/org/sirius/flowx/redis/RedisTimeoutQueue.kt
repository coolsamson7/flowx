package org.sirius.flowx.redis
/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.sirius.flowx.TimeoutEntry
import org.sirius.flowx.TimeoutQueue
import org.springframework.data.redis.core.StringRedisTemplate

class RedisTimeoutQueue(private val redis: StringRedisTemplate) : TimeoutQueue {

    private val key = "saga-timeouts"

    override suspend fun scheduleTimeout(sagaId: String, stepId: String, timeoutAt: Long) {
        withContext(Dispatchers.IO) {
            redis.opsForZSet().add(key, "$sagaId:$stepId", timeoutAt.toDouble())
        }
    }

    override suspend fun popExpired(now: Long, limit: Int): List<TimeoutEntry> {
        return withContext(Dispatchers.IO) {
            val expired = redis.opsForZSet()
                .rangeByScore(key, 0.0, now.toDouble(), 0, limit.toLong())
                ?.toList()
                ?: emptyList()

            if (expired.isNotEmpty()) {
                redis.opsForZSet().remove(key, *expired.toTypedArray())
            }

            expired.map { it ->
                val parts = it.split(":")
                TimeoutEntry(parts[0], parts[1])
            }
        }
    }
}