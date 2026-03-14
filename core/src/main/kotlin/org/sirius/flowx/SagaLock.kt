package org.sirius.flowx

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import kotlinx.coroutines.sync.Mutex
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

interface SagaLock {
    /**
     * Acquires the lock for [sagaId], runs [block], then releases it.
     *
     * Returns null if the lock is already held (contention).
     * Callers must treat null as "skipped this tick, will retry".
     */
    suspend fun <T> withLock(sagaId: String, block: suspend () -> T): T?
}

/**
 * In-process lock — safe within a single JVM, not across a cluster.
 * For cluster use, swap in RedisSagaLock.
 *
 * FIX: contention is now logged at DEBUG so it is visible during
 * load testing without flooding logs in normal operation.
 * Previously a missed lock was a completely silent no-op, making
 * it impossible to distinguish "skipped due to contention" from
 * "work was never scheduled".
 */
class LocalSagaLock : SagaLock {

    private val logger = LoggerFactory.getLogger(LocalSagaLock::class.java)
    private val locks  = ConcurrentHashMap<String, Mutex>()

    override suspend fun <T> withLock(sagaId: String, block: suspend () -> T): T? {
        val mutex = locks.computeIfAbsent(sagaId) { Mutex() }

        if (!mutex.tryLock()) {
            logger.debug("Lock contention on saga $sagaId — skipping this tick")
            return null
        }

        return try {
            block()
        } finally {
            mutex.unlock()
        }
    }
}