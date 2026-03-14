package org.sirius.flowx

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The single runner loop for the saga engine.
 *
 * FIX: SagaEngine no longer has its own internal runner loop.
 * This class is the only driver of processPendingEvents / processActiveSagas,
 * so work is never processed twice per tick.
 *
 * FIX: start() is triggered by ApplicationReadyEvent, not @PostConstruct,
 * guaranteeing the full Spring context (including EntityManagerFactory) is
 * ready before we touch the database in engine.initialize().
 */
@Component
class SagaEngineRunner(
    private val engine: SagaEngine,
    private val eventStore: SagaEventStore   // ← inject directly
) {
    private val logger  = LoggerFactory.getLogger(SagaEngineRunner::class.java)
    private val running = AtomicBoolean(false)
    private var job: Job? = null

    @EventListener(ApplicationReadyEvent::class)
    @Order(1)
    fun start() {
        if (running.compareAndSet(false, true)) {
            engine.initialize()

            job = CoroutineScope(Dispatchers.Default).launch {
                logger.info("SagaEngineRunner started")
                var staleSweepTick = 0

                while (running.get()) {
                    try {
                        // 1. Deliver pending events to waiting saga steps
                        engine.processPendingEvents()

                        // 2. Advance any sagas with ready (token >= 1) steps
                        engine.processActiveSagas()

                        // 3. Release stale event claims every ~30 s so events
                        //    held by crashed nodes are retried by a live node
                        if (++staleSweepTick % 600 == 0) {
                            withContext(Dispatchers.IO) {
                                try {
                                    eventStore.releaseStaleClaims(olderThanMs = 30_000)
                                } catch (ex: Throwable) {
                                    logger.warn("Stale-claim sweep failed", ex)
                                }
                            }
                        }

                        delay(50)
                    } catch (ex: Exception) {
                        logger.error("Error in SagaEngineRunner loop", ex)
                    }
                }
            }
        }
    }

    fun stop() {
        if (running.compareAndSet(true, false)) {
            runBlocking { job?.cancelAndJoin() }
            logger.info("SagaEngineRunner stopped")
        }
    }
}