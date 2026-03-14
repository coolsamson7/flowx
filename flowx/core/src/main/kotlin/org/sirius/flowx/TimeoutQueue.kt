package org.sirius.flowx
/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.PriorityQueue

data class TimeoutEntry(
    val sagaId: String,
    val stepId: String
)

interface TimeoutQueue {
    suspend fun scheduleTimeout(
        sagaId: String,
        stepId: String,
        timeoutAt: Long
    )

    suspend fun popExpired(
        now: Long,
        limit: Int = 100
    ): List<TimeoutEntry>
}

class LocalTimeoutQueue : TimeoutQueue {

    private data class Item(
        val sagaId: String,
        val stepId: String,
        val timeoutAt: Long
    )

    private val queue = PriorityQueue<Item>(compareBy { it.timeoutAt })
    private val mutex = Mutex()

    override suspend fun scheduleTimeout(
        sagaId: String,
        stepId: String,
        timeoutAt: Long
    ) {

        mutex.withLock {
            queue.add(Item(sagaId, stepId, timeoutAt))
        }
    }

    override suspend fun popExpired(
        now: Long,
        limit: Int
    ): List<TimeoutEntry> {

        return mutex.withLock {

            val result = mutableListOf<TimeoutEntry>()

            while (queue.isNotEmpty()
                && queue.peek().timeoutAt <= now
                && result.size < limit
            ) {

                val item = queue.poll()

                result.add(
                    TimeoutEntry(
                        item.sagaId,
                        item.stepId
                    )
                )
            }

            result
        }
    }
}