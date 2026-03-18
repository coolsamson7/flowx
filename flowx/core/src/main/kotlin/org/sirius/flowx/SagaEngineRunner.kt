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
                while (running.get()) {
                    try {
                        engine.processPendingEvents()
                        engine.processActiveSagas()
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