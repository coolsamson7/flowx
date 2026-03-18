package org.sirius.flowx

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.annotation.PostConstruct
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
    private val eventStore: SagaEventStore
) : DisposableBean, EventSink {

    private val logger = LoggerFactory.getLogger(SagaEngine::class.java)
    val instanceId     = UUID.randomUUID().toString()

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val cache    = ConcurrentHashMap<String, Pair<LoadedSaga<Saga<*>>, Long>>()
    private val cacheTTL = 5 * 60 * 1000L

    private val mapper = jacksonObjectMapper()

    // --------------------------------------------------
    // Lifecycle
    // --------------------------------------------------

    @PostConstruct
    fun bindDispatcher() {
        EventDispatcher.bind(this)
    }

    fun initialize() {
        scope.launch(Dispatchers.IO) { restoreActiveSagas() }
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
        val elseTargetIds = instance.typedNodes()
            .filterIsInstance<ConditionNode<*>>()
            .mapNotNull { it.elseTarget?.id }
            .toSet()

        val stepState = mutableMapOf<String, StepState>()
        instance.typedNodes().forEach { node ->
            stepState[node.id] = StepState().apply {
                tokenCount = if (node.predecessors.isEmpty() && node.id !in elseTargetIds) 1 else 0
            }
        }
        val runtime = LoadedSaga<Saga<*>>(instance, stepState, CompletableDeferred())
        putInCache(runtime)
        scheduleReadyNodes(instance.id)
        return instance
    }

    fun onComplete(sagaId: String, callback: (Boolean) -> Unit) {
        val runtime = fromCache(sagaId) ?: return
        scope.launch { callback(runtime.completion.await()) }
    }

    // --------------------------------------------------
    // Runner entry points — recovery only, not primary execution
    // --------------------------------------------------

    /**
     * Recover RUNNING/COMPENSATING sagas not in this node's cache.
     * Sorted by lastProcessedAt ASC so orphans from crashed nodes and
     * starved sagas float to the top naturally.
     * Redis sagaLock ensures only one node executes each saga.
     */
    suspend fun recoverStuckSagas(pageSize: Int = 100) {
        val candidates = withContext(Dispatchers.IO) {
            storage.findLeastRecentlyProcessed(pageSize)
        }

        candidates.filter { !cache.containsKey(it) }.forEach { sagaId ->
            scope.launch {
                sagaLock.withLock(sagaId) {
                    if (cache.containsKey(sagaId)) return@withLock

                    val loaded = withContext(Dispatchers.IO) {
                        runCatching { storage.load(sagaId, factory) }.getOrNull()
                    } ?: return@withLock

                    val needsWork = loaded.stepState.values.any {
                        it.status in setOf(
                            StepStatus.PENDING,
                            StepStatus.RUNNING,
                            StepStatus.COMPENSATING
                        )
                    }
                    if (!needsWork) return@withLock

                    putInCache(loaded)

                    loaded.stepState.entries
                        .filter { it.value.status == StepStatus.AWAITING_EVENT
                                && it.value.timeoutAt != null }
                        .forEach { (stepId, state) ->
                            scheduleTimeout(sagaId, stepId, state.timeoutAt!!)
                        }

                    scheduleReadyNodes(sagaId)
                }
            }
        }
    }

    /**
     * Recover AWAITING_EVENT sagas that have PENDING events in the event store.
     * These represent dropped work — a node stored the event but crashed before
     * calling drainPendingEvents(). Does NOT load ALL awaiting sagas — only
     * those with concrete evidence of a pending event.
     */
    suspend fun recoverPendingEvents(pageSize: Int = 100) {
        val candidates = withContext(Dispatchers.IO) {
            storage.findAwaitingWithPendingEvents(pageSize)
        }

        candidates.forEach { sagaId ->
            scope.launch {
                sagaLock.withLock(sagaId) {
                    drainPendingEvents(sagaId)
                }
            }
        }
    }

    // --------------------------------------------------
    // Restore on startup
    // --------------------------------------------------

    private suspend fun restoreActiveSagas() {
        for (sagaId in storage.findActiveSagaIds()) {
            try {
                val loaded = storage.load(sagaId, factory)
                putInCache(loaded)

                val compensating = loaded.stepState.values
                    .filter { it.status == StepStatus.COMPENSATING }

                if (compensating.isNotEmpty()) {
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
                    loaded.stepState.entries
                        .filter { it.value.status == StepStatus.AWAITING_EVENT
                                && it.value.timeoutAt != null }
                        .forEach { (stepId, state) ->
                            scheduleTimeout(sagaId, stepId, state.timeoutAt!!)
                        }
                    scheduleReadyNodes(sagaId)
                }
            } catch (ex: Throwable) {
                logger.error("Failed to restore saga $sagaId", ex)
            }
        }
    }

    // --------------------------------------------------
    // Timeout — exact coroutine per timeout, no polling
    // --------------------------------------------------

    private fun scheduleTimeout(sagaId: String, stepId: String, timeoutAt: Long) {
        scope.launch {
            val delayMs = timeoutAt - System.currentTimeMillis()
            if (delayMs > 0) delay(delayMs)

            sagaLock.withLock(sagaId) {
                val runtime = getOrLoad(sagaId) ?: return@withLock
                val st = runtime.stepState[stepId] ?: return@withLock
                if (st.status == StepStatus.AWAITING_EVENT) {
                    logger.warn("Step '$stepId' of saga '$sagaId' timed out")
                    failSaga(runtime, stepId)
                }
            }
        }
    }

    // --------------------------------------------------
    // Event drain — sagaLock serialises, no DB-level locking needed
    // --------------------------------------------------
    private suspend fun drainPendingEvents(sagaId: String) {
        val pending = withContext(Dispatchers.IO) {
            try { eventStore.getPending(sagaId) }
            catch (ex: Throwable) {
                logger.error("Failed to fetch pending events for saga $sagaId", ex)
                emptyList()
            }
        }
        if (pending.isEmpty()) return

        val runtime = getOrLoad(sagaId) ?: return

        for (pendingEvent in pending) {
            val event = try {
                pendingEvent.deserialize(mapper)
            } catch (ex: Throwable) {
                logger.error("Cannot deserialise event ${pendingEvent.id}: ${ex.message}")
                withContext(Dispatchers.IO) { eventStore.markFailed(pendingEvent.id, ex.message) }
                continue
            }

            val node = runtime.instance.typedNodes()
                .find { runtime.stepState[it.id]?.status == StepStatus.AWAITING_EVENT }

            if (node == null) continue

            if (!node.handlers.containsKey(event::class.java)) {
                logger.warn("No handler on node '${node.id}' for ${event::class.simpleName}")
                continue
            }

            // Deliver inline — caller already holds sagaLock
            deliverEvent(event, node, runtime)
            withContext(Dispatchers.IO) {
                try { eventStore.markProcessed(pendingEvent.id) }
                catch (ex: Throwable) {
                    logger.warn("Failed to mark event ${pendingEvent.id} processed", ex)
                }
            }
        }
    }

    // dispatch — wrap drainPendingEvents in sagaLock so delivery is serialised
    override fun dispatch(sagaId: String, event: Event) {
        scope.launch(Dispatchers.IO) {
            try {
                eventStore.store(event)
            } catch (ex: Throwable) {
                logger.error("Failed to persist event ${event::class.simpleName} for saga $sagaId", ex)
                return@launch
            }

            // Wait for the lock — event delivery must not be skipped.
            // If executeNode currently holds the lock, we wait until it finishes,
            // then drain. drainPendingEvents handles the case where no step is
            // AWAITING_EVENT yet (event stays PENDING, inline drain picks it up).
            sagaLock.withLockWaiting(sagaId) {
                drainPendingEvents(sagaId)
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
            logger.debug("Evicted saga $sagaId from cache")
        }
    }

    /**
     * Park AWAITING_EVENT sagas — evict from cache after idle period.
     * They consume memory but need no CPU until an event arrives.
     * dispatch() → getOrLoad() will reload them transparently when needed.
     * recoverPendingEvents() handles the crash-recovery case.
     */
    private fun parkAwaitingEvent(sagaId: String) {
        scope.launch {
            delay(60_000)   // 1 min idle → evict
            val runtime = fromCache(sagaId) ?: return@launch
            val allParked = runtime.instance.typedNodes()
                .filter { !isDone(runtime.stepState[it.id]?.status) }
                .all { runtime.stepState[it.id]?.status == StepStatus.AWAITING_EVENT }
            if (allParked) {
                cache.remove(sagaId)
                logger.debug("Parked saga $sagaId evicted (AWAITING_EVENT, idle)")
            }
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
                    .filter { node -> isReady(node, runtime.stepState[node.id]!!) }
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

        val alreadyClaimed = runtime.mutex.withLock {
            val st = runtime.stepState[node.id]!!
            if (st.status != StepStatus.PENDING) {
                true
            } else {
                st.status = StepStatus.RUNNING
                false
            }
        }
        if (alreadyClaimed) return false

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

    /**
     * Recursively mark branch subtree nodes as SKIPPED, stopping at the join.
     * Called when a condition gate evaluates — nodes in the untaken path must
     * be SKIPPED so all { isDone } can return true and completeSaga fires.
     */
    private fun skipBranchSubtree(
        node: Node<*>,
        runtime: LoadedSaga<Saga<*>>,
        joinNodeId: String
    ) {
        if (node.id == joinNodeId) return
        val st = runtime.stepState[node.id] ?: return
        if (st.status != StepStatus.PENDING) return
        st.status = StepStatus.SKIPPED
        node.successors.forEach { skipBranchSubtree(it, runtime, joinNodeId) }
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun <T : Saga<*>> executeStep(
        sagaId: String,
        node: Node<T>,
        runtime: LoadedSaga<Saga<*>>
    ) {
        if (node is ConditionNode<*>) {
            val condNode = node as ConditionNode<T>
            val matches  = condNode.branchCondition(runtime.instance as T)

            if (matches) {
                runtime.mutex.withLock {
                    // Skip all remaining chained gates AND their branch subtrees
                    var next: Node<*>? = condNode.elseTarget
                    while (next is ConditionNode<*>) {
                        val nextCond = next as ConditionNode<*>
                        runtime.stepState[next.id]!!.status = StepStatus.SKIPPED
                        nextCond.successors.forEach { successor ->
                            skipBranchSubtree(successor, runtime, condNode.joinNodeId)
                        }
                        next = nextCond.elseTarget
                    }
                    persist(runtime)
                }
                completeNode(sagaId, node)
            } else {
                val target = condNode.elseTarget
                    ?: error("ConditionNode '${node.id}' has no elseTarget")

                runtime.mutex.withLock {
                    runtime.stepState[node.id]!!.status = StepStatus.SKIPPED
                    // Skip branch steps of THIS gate — the path not taken
                    node.successors.forEach { successor ->
                        skipBranchSubtree(successor, runtime, condNode.joinNodeId)
                    }
                    runtime.stepState[target.id]!!.tokenCount += 1
                    persist(runtime)
                }

                if (isReady(target, runtime.stepState[target.id]!!)) {
                    scope.launch {
                        sagaLock.withLock(sagaId) { executeNode(sagaId, target) }
                    }
                }
            }
            return
        }

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
                val st       = runtime.stepState[node.id]!!
                st.status    = StepStatus.AWAITING_EVENT
                st.timeoutAt = timeoutAt
                persist(runtime)
            }
            if (timeoutAt != null) scheduleTimeout(sagaId, node.id, timeoutAt)
            drainPendingEvents(sagaId)
            // Park after drain — evict from cache if no event arrives soon
            parkAwaitingEvent(sagaId)
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

                runtime.instance.typedNodes()
                    .filter { n -> isReady(n, runtime.stepState[n.id]!!) }
            }

            if (runtime.instance.typedNodes()
                    .all { isDone(runtime.stepState[it.id]?.status) }) {
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
        val loaded = withContext(Dispatchers.IO) {
            runCatching { storage.load(sagaId, factory) }.getOrNull()
        } ?: return null
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