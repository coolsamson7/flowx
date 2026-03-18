package org.sirius.flowx

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

interface SagaLock {
    /** Try-once — returns null on contention. Used by recovery scans (skipping is OK). */
    suspend fun <T> withLock(sagaId: String, block: suspend () -> T): T?

    /** Waits for the lock — used for event delivery where skipping is not OK. */
    suspend fun <T> withLockWaiting(sagaId: String, block: suspend () -> T): T
}

class LocalSagaLock : SagaLock {

    private val logger = LoggerFactory.getLogger(LocalSagaLock::class.java)
    private val locks  = ConcurrentHashMap<String, Mutex>()

    override suspend fun <T> withLock(sagaId: String, block: suspend () -> T): T? {
        val mutex = locks.computeIfAbsent(sagaId) { Mutex() }
        if (!mutex.tryLock()) {
            logger.debug("Lock contention on saga $sagaId — skipping this tick")
            return null
        }
        return try { block() } finally { mutex.unlock() }
    }

    override suspend fun <T> withLockWaiting(sagaId: String, block: suspend () -> T): T {
        val mutex = locks.computeIfAbsent(sagaId) { Mutex() }
        return mutex.withLock { block() }   // suspends until available, never skips
    }
}