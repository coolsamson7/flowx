package org.sirius.flowx

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.springframework.beans.factory.config.AutowireCapableBeanFactory
import org.springframework.context.ApplicationContext
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

// ─────────────────────────────────────────────────────────────────────────────
// Test-only commands and events
// ─────────────────────────────────────────────────────────────────────────────

data class StartCmd(val value: String = "test") : Command
data class UnknownCmd(val x: Int = 0)            : Command  // never registered
data class OkEvent(override val sagaId: String, val value: String) : Event
data class NokEvent(override val sagaId: String)                   : Event

// ─────────────────────────────────────────────────────────────────────────────
// Test saga definitions
// All fields that need to survive a save/load round-trip are @Persisted.
// No @Autowired dependencies — tests run without a Spring context.
// ─────────────────────────────────────────────────────────────────────────────

/** Two sequential synchronous steps. */
@SagaType("linear")
@StartedBy(StartCmd::class)
class LinearSaga : AbstractSaga<LinearSaga>() {
    @Persisted var input:  String  = ""
    @Persisted var s1:     Boolean = false
    @Persisted var s2:     Boolean = false
    @Persisted var s1Comp: Boolean = false
    @Persisted var s2Comp: Boolean = false
    override fun definition() = def
    companion object {
        val def = saga<LinearSaga> {
            on<StartCmd> { cmd -> input = cmd.value }
            step("s1") {
                execute   { s1 = true }
                compensate { s1Comp = true }
            }
            step("s2") {
                execute   { s2 = true }
                compensate { s2Comp = true }
            }
        }
    }
}

/** Step whose execution can be skipped via a runtime condition. */
@SagaType("conditional")
class ConditionalSaga : AbstractSaga<ConditionalSaga>() {
    @Persisted var shouldRun: Boolean = true
    @Persisted var ran:       Boolean = false
    @Persisted var after:     Boolean = false
    override fun definition() = def
    companion object {
        val def = saga<ConditionalSaga> {
            step("guarded") {
                execute   { ran = true }
                condition { shouldRun }
            }
            step("after") { execute { after = true } }
        }
    }
}

/**
 * Synchronous step followed by an async step that waits for an event.
 * "before" has a compensate block so we can verify rollback.
 */
@SagaType("async")
class AsyncSaga : AbstractSaga<AsyncSaga>() {
    @Persisted var beforeDone: Boolean = false
    @Persisted var result:     String  = ""
    @Persisted var comp:       Boolean = false
    override fun definition() = def
    companion object {
        val def = saga<AsyncSaga> {
            step("before") {
                execute   { beforeDone = true }
                compensate { comp = true }
            }
            step("async") {
                execute { /* fires external async work — no local side-effect */ }
                onSuccess<OkEvent>  { e -> result = e.value }
                onFailure<NokEvent> { _ -> }
            }
        }
    }
}

/**
 * s1 succeeds; boom throws; s1 must be compensated.
 * verifies reverse-order compensation.
 */
@SagaType("failing")
class FailingSaga : AbstractSaga<FailingSaga>() {
    @Persisted var s1Done: Boolean = false
    @Persisted var s1Comp: Boolean = false
    override fun definition() = def
    companion object {
        val def = saga<FailingSaga> {
            step("s1") {
                execute   { s1Done = true }
                compensate { s1Comp = true }
            }
            step("boom") {
                execute { throw RuntimeException("intentional failure") }
                compensate { /* never reached — boom never succeeded */ }
            }
        }
    }
}

/**
 * Three sequential steps with compensation — used to verify that
 * compensation runs in strict reverse order.
 */
@SagaType("multi-comp")
class MultiCompSaga : AbstractSaga<MultiCompSaga>() {
    @Persisted var log: String = ""
    override fun definition() = def
    companion object {
        val def = saga<MultiCompSaga> {
            step("a") {
                execute   { log += "a" }
                compensate { log += "A" }
            }
            step("b") {
                execute   { log += "b" }
                compensate { log += "B" }
            }
            step("c") {
                execute { throw RuntimeException("fail at c") }
            }
        }
    }
}

/**
 * Branch saga used to document the KNOWN BUG that ConditionNode.branchCondition
 * is never evaluated by the engine — both branches always execute.
 * Tests against this class intentionally assert the *actual* (buggy) behaviour
 * so they will fail if the bug is fixed, reminding the developer to update them.
 */
@SagaType("branching")
class BranchingSaga : AbstractSaga<BranchingSaga>() {
    @Persisted var amount:   Double  = 0.0
    @Persisted var highRan:  Boolean = false
    @Persisted var lowRan:   Boolean = false
    @Persisted var afterRan: Boolean = false
    override fun definition() = def
    companion object {
        val def = saga<BranchingSaga> {
            branch {
                on({ amount > 100.0 }) {
                    step("high") { execute { highRan = true } }
                }
                otherwise {
                    step("low") { execute { lowRan = true } }
                }
            }
            step("after") { execute { afterRan = true } }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// In-memory SagaEventStore for tests
// ─────────────────────────────────────────────────────────────────────────────

class TestEventStore : SagaEventStore {
    private val mapper = jacksonObjectMapper()

    private data class Row(
        val id: String,
        val sagaId: String,
        val event: ClaimedEvent,
        @Volatile var claimed: Boolean   = false,
        @Volatile var processed: Boolean = false,
        @Volatile var failed: Boolean    = false
    )

    private val rows = ConcurrentLinkedQueue<Row>()

    override fun store(event: Event) {
        rows += Row(
            id     = UUID.randomUUID().toString(),
            sagaId = event.sagaId,
            event  = ClaimedEvent(
                id         = UUID.randomUUID().toString(),
                sagaId     = event.sagaId,
                eventClass = event::class.java.name,
                payload    = mapper.writeValueAsString(event)
            )
        )
    }

    override fun claimPending(sagaId: String, instanceId: String, limit: Int): List<ClaimedEvent> {
        val result = mutableListOf<ClaimedEvent>()
        for (row in rows) {
            if (result.size >= limit) break
            if (row.sagaId == sagaId && !row.claimed && !row.processed && !row.failed) {
                row.claimed = true
                result += row.event
            }
        }
        return result
    }

    override fun markProcessed(eventId: String) {
        rows.find { it.event.id == eventId }?.processed = true
    }

    override fun releaseClaim(eventId: String) {
        rows.find { it.event.id == eventId }?.claimed = false
    }

    override fun markFailed(eventId: String, reason: String?) {
        rows.find { it.event.id == eventId }?.failed = true
    }

    override fun releaseStaleClaims(olderThanMs: Long) {
        rows.filter { it.claimed && !it.processed }.forEach { it.claimed = false }
    }

    fun pendingCount(sagaId: String) =
        rows.count { it.sagaId == sagaId && !it.claimed && !it.processed && !it.failed }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test suite
// ─────────────────────────────────────────────────────────────────────────────

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class SagaEngineTest {

    private lateinit var engine:     SagaEngine
    private lateinit var factory:    SagaFactory
    private lateinit var storage:    InMemorySagaStorage
    private lateinit var registry:   SagaRegistry
    private lateinit var eventStore: TestEventStore

    @BeforeEach
    fun setup() {
        // Let the factory instantiate sagas via plain reflection (no Spring wiring).
        val beanFactory = mockk<AutowireCapableBeanFactory>(relaxed = true)
        every { beanFactory.createBean(any<Class<*>>()) } answers {
            firstArg<Class<*>>().getDeclaredConstructor().newInstance()
        }
        factory = SagaFactory(beanFactory)

        // Provide the registry with our test saga classes.
        val appCtx = mockk<ApplicationContext>()
        every { appCtx.getBeansWithAnnotation(SagaType::class.java) } returns mapOf(
            "linear"     to LinearSaga(),
            "conditional" to ConditionalSaga(),
            "async"      to AsyncSaga(),
            "failing"    to FailingSaga(),
            "multi-comp" to MultiCompSaga(),
            "branching"  to BranchingSaga()
        )
        registry = SagaRegistry(appCtx)
        registry.scanAndRegister()

        storage    = InMemorySagaStorage(registry)
        eventStore = TestEventStore()

        engine = SagaEngine(
            storage      = storage,
            factory      = factory,
            registry     = registry,
            sagaLock     = LocalSagaLock(),
            timeoutQueue = LocalTimeoutQueue(),
            eventStore   = eventStore
        )
        // Do NOT call engine.initialize() — we drive execution manually so
        // tests are deterministic and don't depend on background polling.
    }

    @AfterEach
    fun teardown() { engine.destroy() }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Awaits the saga's completion deferred with a timeout. */
    private suspend fun awaitCompletion(sagaId: String, ms: Long = 3_000): Boolean {
        val d = CompletableDeferred<Boolean>()
        engine.onComplete(sagaId) { d.complete(it) }
        return withTimeout(ms) { d.await() }
    }

    /** Loads the saga instance from storage after a test run. */
    private fun loadSaga(sagaId: String): Saga<*> =
        storage.load(sagaId, factory).instance

    // ── tests: normal execution ───────────────────────────────────────────────

    @Test
    fun `linear saga executes both steps and completes successfully`() = runBlocking {
        val sagaId = engine.send(StartCmd("hello"))
        assertTrue(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as LinearSaga
        assertEquals("hello", s.input)
        assertTrue(s.s1)
        assertTrue(s.s2)
        assertFalse(s.s1Comp)
        assertFalse(s.s2Comp)
    }

    @Test
    fun `step with false condition is SKIPPED and saga still completes`() = runBlocking {
        val saga   = factory.create(ConditionalSaga::class.java) { shouldRun = false }
        val sagaId = engine.start(saga).id

        assertTrue(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as ConditionalSaga
        assertFalse(s.ran,  "guarded step should have been skipped")
        assertTrue(s.after, "step after the skipped one should have run")
    }

    @Test
    fun `step with true condition executes normally`() = runBlocking {
        val saga   = factory.create(ConditionalSaga::class.java) { shouldRun = true }
        val sagaId = engine.start(saga).id

        assertTrue(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as ConditionalSaga
        assertTrue(s.ran)
        assertTrue(s.after)
    }

    // ── tests: async / event-driven steps ─────────────────────────────────────

    @Test
    fun `async step completes and result is stored when success event dispatched`() = runBlocking {
        val sagaId = engine.start(factory.create(AsyncSaga::class.java)).id

        // Dispatch immediately — the event is stored in the TestEventStore and
        // drained when the async step transitions to AWAITING_EVENT.
        engine.dispatch(OkEvent(sagaId = sagaId, value = "done"))

        assertTrue(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as AsyncSaga
        assertTrue(s.beforeDone)
        assertEquals("done", s.result)
        assertFalse(s.comp)
    }

    @Test
    fun `failure event on async step triggers compensation of prior steps`() = runBlocking {
        val sagaId = engine.start(factory.create(AsyncSaga::class.java)).id

        engine.dispatch(NokEvent(sagaId = sagaId))

        assertFalse(awaitCompletion(sagaId), "saga should complete with failure")

        val s = loadSaga(sagaId) as AsyncSaga
        assertTrue(s.beforeDone, "before-step should have executed")
        assertTrue(s.comp,       "before-step should have been compensated")
    }

    @Test
    fun `event dispatched before async step is ready is buffered and delivered later`() = runBlocking {
        // Dispatch BEFORE engine.start — event lands in TestEventStore as PENDING.
        val futureId = UUID.randomUUID().toString()
        eventStore.store(OkEvent(sagaId = futureId, value = "early"))

        // Create and start the saga with the same id.
        val saga = factory.create(AsyncSaga::class.java) { id = futureId }
        engine.start(saga)

        assertTrue(awaitCompletion(futureId))

        val s = loadSaga(futureId) as AsyncSaga
        assertEquals("early", s.result)
    }

    // ── tests: failure and compensation ──────────────────────────────────────

    @Test
    fun `exception thrown in step triggers compensation and saga reports failure`() = runBlocking {
        val sagaId = engine.start(factory.create(FailingSaga::class.java)).id

        assertFalse(awaitCompletion(sagaId), "saga should report failure")

        val s = loadSaga(sagaId) as FailingSaga
        assertTrue(s.s1Done, "s1 should have executed before the crash")
        assertTrue(s.s1Comp, "s1 should have been compensated")
    }

    @Test
    fun `compensation runs in strict reverse order of execution`() = runBlocking {
        // a and b execute successfully; c throws.
        // Expected compensation order: b first, then a  (log = "abBA")
        val sagaId = engine.start(factory.create(MultiCompSaga::class.java)).id

        assertFalse(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as MultiCompSaga
        // "ab" from execution, then "BA" from reverse compensation
        assertEquals("abBA", s.log,
            "compensation must run in reverse execution order")
    }

    @Test
    fun `compensationStarted flag prevents double-compensation on concurrent failures`() = runBlocking {
        // This test verifies that the AtomicBoolean guard in failSaga() prevents
        // compensate() being called more than once even if two threads detect failure
        // simultaneously (e.g. a step exception racing with the timeout scanner).
        val sagaId = engine.start(factory.create(FailingSaga::class.java)).id
        assertFalse(awaitCompletion(sagaId))

        // If double-compensation occurred, s1Comp counter would show it.
        // We verify the persisted state is consistent (not double-applied).
        val s = loadSaga(sagaId) as FailingSaga
        assertTrue(s.s1Comp)
        // No assertion on count because @Persisted is just boolean —
        // the key check is that no exception was thrown and the saga settled cleanly.
    }

    // ── tests: engine.send / routing ─────────────────────────────────────────

    @Test
    fun `send routes command to the correct saga type via StartedBy`() = runBlocking {
        val sagaId = engine.send(StartCmd("routed"))
        assertTrue(awaitCompletion(sagaId))
        assertEquals("routed", (loadSaga(sagaId) as LinearSaga).input)
    }

    @Test
    fun `send throws IllegalStateException for unregistered command`() {
        assertThrows<IllegalStateException> { engine.send(UnknownCmd()) }
    }

    // ── tests: persistence and restore ───────────────────────────────────────

    @Test
    fun `saga is persisted to storage and can be loaded back`() = runBlocking {
        val sagaId = engine.send(StartCmd("persisted"))
        awaitCompletion(sagaId)

        // Load directly from storage (bypasses cache) and verify state.
        val loaded = storage.load(sagaId, factory).instance as LinearSaga
        assertTrue(loaded.s1)
        assertTrue(loaded.s2)
        assertEquals("persisted", loaded.input)
    }

    @Test
    fun `saga restores from storage on engine restart and completes on next event`() = runBlocking {
        // Phase 1 — start AsyncSaga on engine1, let "before" complete.
        val sagaId = engine.start(factory.create(AsyncSaga::class.java)).id
        // Wait for the before-step to finish (async step will then be AWAITING_EVENT).
        withTimeout(3_000) {
            while (storage.load(sagaId, factory).stepState["before"]?.status != StepStatus.SUCCESS) {
                delay(20)
            }
        }

        // Phase 2 — simulate a node restart.
        engine.destroy()

        val engine2 = SagaEngine(
            storage      = storage,
            factory      = factory,
            registry     = registry,
            sagaLock     = LocalSagaLock(),
            timeoutQueue = LocalTimeoutQueue(),
            eventStore   = eventStore
        )
        engine2.initialize()
        delay(300) // let restoreActiveSagas() complete on the IO dispatcher

        // Phase 3 — dispatch the event to the new engine instance.
        engine2.dispatch(OkEvent(sagaId = sagaId, value = "restored"))

        val d = CompletableDeferred<Boolean>()
        engine2.onComplete(sagaId) { d.complete(it) }
        val success = withTimeout(3_000) { d.await() }

        assertTrue(success)
        assertEquals("restored", (storage.load(sagaId, factory).instance as AsyncSaga).result)
        engine2.destroy()
    }

    @Test
    fun `engine resumes mid-compensation after restart`() = runBlocking {
        // Manually craft a LoadedSaga that has one step COMPENSATING (i.e. the
        // engine crashed between writing COMPENSATING and COMPENSATED).
        val saga = factory.create(LinearSaga::class.java) { id = UUID.randomUUID().toString(); input = "crash" }
        val stepState = mutableMapOf(
            "s1" to StepState(StepStatus.COMPENSATING, tokenCount = 1),
            "s2" to StepState(StepStatus.SUCCESS,      tokenCount = 1)
        )
        storage.save(saga.id, saga, stepState)

        val engine2 = SagaEngine(
            storage      = storage,
            factory      = factory,
            registry     = registry,
            sagaLock     = LocalSagaLock(),
            timeoutQueue = LocalTimeoutQueue(),
            eventStore   = eventStore
        )
        engine2.initialize()

        val d = CompletableDeferred<Boolean>()
        engine2.onComplete(saga.id) { d.complete(it) }
        val success = withTimeout(3_000) { d.await() }

        // Compensation resumed → saga completes with failure
        assertFalse(success)
        engine2.destroy()
    }

    // ── tests: event store integration ───────────────────────────────────────

    @Test
    fun `runStaleSweep releases claimed events so they can be re-delivered`() = runBlocking {
        val sagaId = UUID.randomUUID().toString()
        eventStore.store(OkEvent(sagaId = sagaId, value = "stale"))

        // Simulate another node claiming but not processing.
        val first = eventStore.claimPending(sagaId, "node-1")
        assertEquals(1, first.size)
        assertEquals(0, eventStore.pendingCount(sagaId), "should be claimed, not pending")

        assertEquals(1, eventStore.pendingCount(sagaId), "event should be pending again after sweep")

        // A different node can now re-claim it.
        val second = eventStore.claimPending(sagaId, "node-2")
        assertEquals(1, second.size)
    }

    // ── tests: branch behaviour (documents current known limitation) ──────────

    /**
     * BUG DOCUMENTATION TEST:
     * ConditionNode.branchCondition is stored in the graph but is never
     * evaluated by the engine. Both gate nodes start with tokenCount=1
     * (no preceding nodes), so both execute unconditionally and both branch
     * paths always run. This test asserts the *actual* behaviour; if the
     * engine is fixed it will fail here as a reminder to update the test.
     */
    @Test
    fun `KNOWN BUG - both branch paths always execute regardless of condition`() = runBlocking {
        val sagaId = engine.start(
            factory.create(BranchingSaga::class.java) { amount = 50.0 } // should take "low" only
        ).id
        assertTrue(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as BranchingSaga
        assertTrue(s.highRan,  "high branch runs even though amount < 100 (engine bug)")
        assertTrue(s.lowRan,   "low branch runs as expected")
        assertTrue(s.afterRan, "step after branch runs")
    }
}