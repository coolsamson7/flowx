package org.sirius.flowx

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean

@Component
class SagaEngineRunner(
    private val engine: SagaEngine
) {
    private val logger  = LoggerFactory.getLogger(SagaEngineRunner::class.java)
    private val running = AtomicBoolean(false)
    private var job: Job? = null

    fun start() {
        if (running.compareAndSet(false, true)) {
            engine.initialize()

            job = CoroutineScope(Dispatchers.Default).launch {
                logger.info("SagaEngineRunner started")
                var tick = 0
                while (running.get()) {
                    try {
                        tick++
                        // Every 5s — drain pending events for AWAITING_EVENT sagas
                        // whose node crashed between store() and drain()
                        // More frequent than stuck scan because event latency is user-visible
                        if (tick % 100 == 0) {
                            engine.recoverPendingEvents()
                        }
                        // Every 10s — recover stuck RUNNING/COMPENSATING sagas
                        // sorted by lastProcessedAt ASC so orphans/starved float to top
                        if (tick % 200 == 0) {
                            engine.recoverStuckSagas()
                        }
                        delay(50)
                    } catch (ex: Exception) {
                        logger.error("Error in SagaEngineRunner", ex)
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