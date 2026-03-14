package org.sirius.flowx

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.beans.factory.DisposableBean
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Component
class SagaEngine(
    private val storage: SagaStorage,
    private val factory: SagaFactory,
    private val registry: SagaRegistry,
    private val sagaLock: SagaLock,
    private val timeoutQueue: TimeoutQueue,
    private val eventStore: SagaEventStore
) : DisposableBean {

    private val logger = LoggerFactory.getLogger(SagaEngine::class.java)

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val cache    = ConcurrentHashMap<String, Pair<LoadedSaga<Saga<*>>, Long>>()
    private val cacheTTL = 5 * 60 * 1000L

    private val instanceId = UUID.randomUUID().toString()
    private val mapper     = jacksonObjectMapper()

    // --------------------------------------------------
    // Lifecycle
    // --------------------------------------------------

    fun initialize() {
        scope.launch(Dispatchers.IO) { restoreActiveSagas() }
        startTimeoutScanner()
    }

    override fun destroy() {
        logger.info("SagaEngine shutting down")
        scope.cancel()
    }

    // --------------------------------------------------
    // Helpers
    // --------------------------------------------------

    @Suppress("UNCHECKED_CAST")
    private fun Saga<*>.typedNodes(): List<Node<Saga<*>>> =
        (definition() as SagaDefinition<Saga<*>>).nodes

    @Suppress("UNCHECKED_CAST")
    private fun Saga<*>.typedDefinition(): SagaDefinition<Saga<*>> =
        definition() as SagaDefinition<Saga<*>>

    private fun isDone(status: StepStatus?) =
        status == StepStatus.SUCCESS || status == StepStatus.SKIPPED

    // FIX: ParallelJoinNode must accumulate requiredTokens before firing.
    // All other nodes fire on the first token.
    private fun isReady(node: Node<*>, st: StepState): Boolean {
        val required = if (node is ParallelJoinNode) node.requiredTokens else 1
        return st.tokenCount >= required && st.status == StepStatus.PENDING
    }

    // --------------------------------------------------
    // Public API
    // --------------------------------------------------

    fun send(command: Command): String {
        val sagaClass = registry.getByCommand(command::class.java)
            ?: throw IllegalStateException(
                "No saga registered for command ${command::class.simpleName}"
            )

        @Suppress("UNCHECKED_CAST")
        val instance = factory.createInternal(sagaClass as Class<Saga<*>>) {
            id = UUID.randomUUID().toString()
        }

        instance.typedDefinition().commandHandler?.invoke(instance, command)
        start(instance)
        return instance.id
    }

    fun <T : Saga<*>> start(instance: T): T {
        val stepState = mutableMapOf<String, StepState>()
        instance.typedNodes().forEach { node ->
            stepState[node.id] = StepState().apply {
                tokenCount = if (node.predecessors.isEmpty()) 1 else 0
            }
        }
        val runtime = LoadedSaga<Saga<*>>(instance, stepState, CompletableDeferred())
        putInCache(runtime)
        scheduleReadyNodes(instance.id)
        return instance
    }

    fun dispatch(event: Event) {
        scope.launch(Dispatchers.IO) {
            try {
                eventStore.store(event)
            } catch (ex: Throwable) {
                logger.error("Failed to persist event ${event::class.simpleName} " +
                        "for saga ${event.sagaId}", ex)
                return@launch
            }

            val runtime = fromCache(event.sagaId)
            if (runtime != null) {
                val awaitingNode = runtime.instance.typedNodes()
                    .find { runtime.stepState[it.id]?.status == StepStatus.AWAITING_EVENT }
                if (awaitingNode != null) drainPendingEvents(event.sagaId)
            }
        }
    }

    fun onComplete(sagaId: String, callback: (Boolean) -> Unit) {
        val runtime = fromCache(sagaId) ?: return
        scope.launch { callback(runtime.completion.await()) }
    }

    // --------------------------------------------------
    // Runner entry points
    // --------------------------------------------------

    suspend fun processPendingEvents() {
        val sagaIds = cache.keys.toList()
        for (sagaId in sagaIds) {
            val runtime = fromCache(sagaId) ?: continue
            val hasAwaitingStep = runtime.instance.typedNodes()
                .any { runtime.stepState[it.id]?.status == StepStatus.AWAITING_EVENT }
            if (!hasAwaitingStep) continue
            drainPendingEvents(sagaId)
        }
    }

    fun processActiveSagas() {
        val activeSagaIds = cache.keys.toList()
        activeSagaIds.forEach { sagaId ->
            val runtime = fromCache(sagaId) ?: return@forEach
            val readyNodes = runtime.instance.typedNodes()
                .filter { node ->
                    val st = runtime.stepState[node.id]!!
                    isReady(node, st)
                }
            readyNodes.forEach { node ->
                scope.launch {
                    sagaLock.withLock(sagaId) { executeNode(sagaId, node) }
                }
            }
        }
    }

    // --------------------------------------------------
    // Restore sagas
    // --------------------------------------------------

    private suspend fun restoreActiveSagas() {
        for (sagaId in storage.findActiveSagaIds()) {
            try {
                val loaded = storage.load(sagaId, factory)
                putInCache(loaded)

                val compensatingSteps =
                    loaded.stepState.values.filter { it.status == StepStatus.COMPENSATING }

                if (compensatingSteps.isNotEmpty()) {
                    logger.warn("Resuming mid-compensation for saga $sagaId")
                    loaded.stepState.entries
                        .filter { it.value.status == StepStatus.COMPENSATING }
                        .forEach { it.value.status = StepStatus.SUCCESS }
                    loaded.compensationStarted.set(true)
                    scope.launch {
                        compensate(loaded)
                        completeSaga(loaded, success = false)
                    }
                } else {
                    scheduleReadyNodes(sagaId)
                }
            } catch (ex: Throwable) {
                logger.error("Failed to restore saga $sagaId", ex)
            }
        }
    }

    // --------------------------------------------------
    // Timeout scanner
    // --------------------------------------------------

    private fun startTimeoutScanner() {
        scope.launch {
            while (true) {
                delay(5_000)
                val expired = timeoutQueue.popExpired(System.currentTimeMillis(), 100)
                for (entry in expired) {
                    sagaLock.withLock(entry.sagaId) {
                        val runtime = getOrLoad(entry.sagaId) ?: return@withLock
                        val node    = runtime.instance.typedNodes()
                            .find { it.id == entry.stepId } ?: return@withLock
                        val st      = runtime.stepState[node.id] ?: return@withLock
                        if (st.status == StepStatus.AWAITING_EVENT) {
                            logger.warn("Step '${node.id}' of saga '${entry.sagaId}' timed out")
                            failSaga(runtime, node.id)
                        }
                    }
                }
            }
        }
    }

    // --------------------------------------------------
    // Pending-event drain
    // --------------------------------------------------

    private suspend fun drainPendingEvents(sagaId: String) {
        val claimed = withContext(Dispatchers.IO) {
            try { eventStore.claimPending(sagaId, instanceId) }
            catch (ex: Throwable) {
                logger.error("Failed to claim events for saga $sagaId", ex)
                emptyList()
            }
        }
        if (claimed.isEmpty()) return

        val runtime = getOrLoad(sagaId)
        if (runtime == null) {
            withContext(Dispatchers.IO) {
                claimed.forEach {
                    try { eventStore.releaseClaim(it.id) }
                    catch (ex: Throwable) { logger.warn("Failed to release claim ${it.id}", ex) }
                }
            }
            return
        }

        for (claimed in claimed) {
            val event = try {
                claimed.deserialize(mapper)
            } catch (ex: Throwable) {
                logger.error("Cannot deserialise event ${claimed.id}: ${ex.message}")
                withContext(Dispatchers.IO) { eventStore.markFailed(claimed.id, ex.message) }
                continue
            }

            val node = runtime.instance.typedNodes()
                .find { runtime.stepState[it.id]?.status == StepStatus.AWAITING_EVENT }

            if (node == null) {
                withContext(Dispatchers.IO) { eventStore.releaseClaim(claimed.id) }
                continue
            }

            if (!node.handlers.containsKey(event::class.java)) {
                logger.warn("No handler on node '${node.id}' for ${event::class.simpleName}")
                withContext(Dispatchers.IO) { eventStore.releaseClaim(claimed.id) }
                continue
            }

            scope.launch {
                sagaLock.withLock(sagaId) {
                    deliverEvent(event, node, runtime)
                    withContext(Dispatchers.IO) {
                        try { eventStore.markProcessed(claimed.id) }
                        catch (ex: Throwable) {
                            logger.warn("Failed to mark event ${claimed.id} as processed", ex)
                        }
                    }
                }
            }
        }
    }

    // --------------------------------------------------
    // Failure / Compensation
    // --------------------------------------------------

    private fun failSaga(runtime: LoadedSaga<Saga<*>>, failedNodeId: String) {
        scope.launch {
            runtime.mutex.withLock {
                runtime.stepState[failedNodeId]!!.status = StepStatus.FAILED
                persist(runtime)
            }
            if (runtime.compensationStarted.compareAndSet(false, true)) {
                compensate(runtime)
                completeSaga(runtime, success = false)
            }
        }
    }

    private suspend fun compensate(runtime: LoadedSaga<Saga<*>>) {
        val toCompensate = runtime.mutex.withLock {
            runtime.stepState.entries
                .filter { it.value.status == StepStatus.SUCCESS }
                .mapNotNull { (id, _) -> runtime.instance.typedNodes().find { it.id == id } }
                .reversed()
        }
        for (node in toCompensate) {
            runtime.mutex.withLock {
                runtime.stepState[node.id]!!.status = StepStatus.COMPENSATING
                persist(runtime)
            }
            node.compensate(runtime.instance)
            runtime.mutex.withLock {
                runtime.stepState[node.id]!!.status = StepStatus.COMPENSATED
                persist(runtime)
            }
        }
    }

    // --------------------------------------------------
    // Completion + cache eviction
    // --------------------------------------------------

    private fun completeSaga(runtime: LoadedSaga<Saga<*>>, success: Boolean) {
        runtime.completion.complete(success)
        scheduleEviction(runtime.instance.id)
    }

    private fun scheduleEviction(sagaId: String) {
        scope.launch {
            delay(cacheTTL)
            cache.remove(sagaId)
            logger.debug("Evicted completed saga $sagaId from cache")
        }
    }

    // --------------------------------------------------
    // Scheduling
    // --------------------------------------------------

    private fun scheduleReadyNodes(sagaId: String) {
        val runtime = fromCache(sagaId) ?: return
        scope.launch {
            val readyNodes = runtime.mutex.withLock {
                runtime.instance.typedNodes()
                    .filter { node ->
                        val st = runtime.stepState[node.id]!!
                        isReady(node, st)
                    }
                    .also { ready ->
                        ready.forEach { runtime.stepState[it.id]!!.status = StepStatus.RUNNING }
                    }
            }
            readyNodes.forEach { node ->
                scope.launch {
                    sagaLock.withLock(sagaId) { executeNode(sagaId, node) }
                }
            }
        }
    }

    // --------------------------------------------------
    // Node execution
    // --------------------------------------------------

    private suspend fun <T : Saga<*>> executeNode(sagaId: String, node: Node<T>): Boolean {
        val runtime = getOrLoad(sagaId) ?: return false
        return try {
            runtime.mutex.withLock { persist(runtime) }
            executeStep(sagaId, node, runtime)
            true
        } catch (ex: Throwable) {
            logger.error("Unexpected error in node '${node.id}'", ex)
            failSaga(runtime, node.id)
            false
        }
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun <T : Saga<*>> executeStep(
        sagaId: String,
        node: Node<T>,
        runtime: LoadedSaga<Saga<*>>
    ) {
        // ------------------------------------------------------------------
        // FIX: ConditionNode — evaluate branchCondition at runtime.
        //
        // true  → complete the gate normally so its successors (branch steps)
        //         receive tokens and execute.
        // false → mark gate SKIPPED, deliver a token to elseTarget
        //         (next gate in chain, or BranchJoinNode when no branch matched).
        // ------------------------------------------------------------------
        if (node is ConditionNode<*>) {
            val condNode = node as ConditionNode<T>
            val matches  = condNode.branchCondition(runtime.instance as T)

            if (matches) {
                // Predicate true — proceed into this branch
                completeNode(sagaId, node)
            } else {
                // Predicate false — skip this branch, advance to elseTarget
                val target = condNode.elseTarget
                    ?: error("ConditionNode '${node.id}' has no elseTarget — DSL wiring bug")

                runtime.mutex.withLock {
                    runtime.stepState[node.id]!!.status = StepStatus.SKIPPED
                    runtime.stepState[target.id]!!.tokenCount += 1
                    persist(runtime)
                }

                val targetSt = runtime.stepState[target.id]!!
                if (isReady(target, targetSt)) {
                    runtime.mutex.withLock {
                        runtime.stepState[target.id]!!.status = StepStatus.RUNNING
                    }
                    scope.launch {
                        sagaLock.withLock(sagaId) { executeNode(sagaId, target) }
                    }
                }
            }
            return
        }

        // ------------------------------------------------------------------
        // Regular node — optional step-level skip condition
        // ------------------------------------------------------------------
        if (node.condition != null && !node.condition!!(runtime.instance as T)) {
            runtime.mutex.withLock {
                runtime.stepState[node.id]!!.status = StepStatus.SKIPPED
                persist(runtime)
            }
            completeNode(sagaId, node)
            return
        }

        node.execute(runtime.instance as T)

        if (node.isAsync) {
            val timeoutAt = node.timeoutMillis?.let { System.currentTimeMillis() + it }
            runtime.mutex.withLock {
                val st = runtime.stepState[node.id]!!
                st.status    = StepStatus.AWAITING_EVENT
                st.timeoutAt = timeoutAt
                persist(runtime)
            }
            if (timeoutAt != null) timeoutQueue.scheduleTimeout(sagaId, node.id, timeoutAt)
            drainPendingEvents(sagaId)
        } else {
            completeNode(sagaId, node)
        }
    }

    // --------------------------------------------------
    // Event delivery
    // --------------------------------------------------

    private fun deliverEvent(event: Event, node: Node<Saga<*>>, runtime: LoadedSaga<Saga<*>>) {
        val handler = node.handlers[event::class.java] ?: return
        when (handler(runtime.instance, event)) {
            EventOutcome.COMPLETE -> completeNode(event.sagaId, node)
            EventOutcome.FAIL     -> failSaga(runtime, node.id)
        }
    }

    // --------------------------------------------------
    // Node completion
    // --------------------------------------------------

    private fun <T : Saga<*>> completeNode(sagaId: String, node: Node<T>) {
        val runtime = fromCache(sagaId) ?: return
        scope.launch {
            val readyToSchedule = runtime.mutex.withLock {
                runtime.stepState[node.id]!!.status = StepStatus.SUCCESS
                node.successors.forEach { succ ->
                    runtime.stepState[succ.id]!!.tokenCount += 1
                }
                persist(runtime)

                // FIX: isReady() respects requiredTokens for ParallelJoinNode
                runtime.instance.typedNodes()
                    .filter { n ->
                        val st = runtime.stepState[n.id]!!
                        isReady(n, st)
                    }
                    .also { ready ->
                        ready.forEach { runtime.stepState[it.id]!!.status = StepStatus.RUNNING }
                    }
            }

            if (runtime.instance.typedNodes().all { isDone(runtime.stepState[it.id]?.status) }) {
                completeSaga(runtime, success = true)
            } else {
                readyToSchedule.forEach { n ->
                    scope.launch {
                        sagaLock.withLock(sagaId) { executeNode(sagaId, n) }
                    }
                }
            }
        }
    }

    // --------------------------------------------------
    // Cache / Storage
    // --------------------------------------------------

    private fun fromCache(sagaId: String): LoadedSaga<Saga<*>>? {
        val cached = cache[sagaId] ?: return null
        cache[sagaId] = cached.copy(second = System.currentTimeMillis())
        return cached.first
    }

    private suspend fun getOrLoad(sagaId: String): LoadedSaga<Saga<*>>? {
        fromCache(sagaId)?.let { return it }
        val loaded = withContext(Dispatchers.IO) { storage.load(sagaId, factory) }
        putInCache(loaded)
        return loaded
    }

    private fun putInCache(runtime: LoadedSaga<Saga<*>>) {
        cache[runtime.instance.id] = runtime to System.currentTimeMillis()
    }

    private suspend fun persist(runtime: LoadedSaga<Saga<*>>) {
        withContext(Dispatchers.IO) {
            storage.save(runtime.instance.id, runtime.instance, runtime.stepState)
        }
    }
}