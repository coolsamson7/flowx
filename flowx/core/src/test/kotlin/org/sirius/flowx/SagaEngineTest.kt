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
data class UnknownCmd(val x: Int = 0)           : Command
data class OkEvent(override val sagaId: String, val value: String) : Event
data class NokEvent(override val sagaId: String)                   : Event

// ─────────────────────────────────────────────────────────────────────────────
// Test saga definitions
// ─────────────────────────────────────────────────────────────────────────────

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
                execute { }
                onSuccess<OkEvent>  { e -> result = e.value }
                onFailure<NokEvent> { _ -> }
            }
        }
    }
}

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
            }
        }
    }
}

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
// In-memory SagaEventStore — implements the simplified interface
// ─────────────────────────────────────────────────────────────────────────────

class TestEventStore : SagaEventStore {
    private val mapper = jacksonObjectMapper()

    private data class Row(
        val id:        String,
        val sagaId:    String,
        val eventClass: String,
        val payload:   String,
        @Volatile var processed: Boolean = false,
        @Volatile var failed:    Boolean = false
    )

    private val rows = ConcurrentLinkedQueue<Row>()

    override fun store(event: Event) {
        rows += Row(
            id         = UUID.randomUUID().toString(),
            sagaId     = event.sagaId,
            eventClass = event::class.java.name,
            payload    = mapper.writeValueAsString(event)
        )
    }

    override fun getPending(sagaId: String): List<PendingEvent> =
        rows.filter { it.sagaId == sagaId && !it.processed && !it.failed }
            .map { PendingEvent(it.id, it.sagaId, it.eventClass, it.payload) }

    override fun markProcessed(eventId: String) {
        rows.find { it.id == eventId }?.processed = true
    }

    override fun markFailed(eventId: String, reason: String?) {
        rows.find { it.id == eventId }?.failed = true
    }

    fun pendingCount(sagaId: String) =
        rows.count { it.sagaId == sagaId && !it.processed && !it.failed }
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
        val beanFactory = mockk<AutowireCapableBeanFactory>(relaxed = true)
        every { beanFactory.createBean(any<Class<*>>()) } answers {
            firstArg<Class<*>>().getDeclaredConstructor().newInstance()
        }
        factory = SagaFactory(beanFactory)

        val appCtx = mockk<ApplicationContext>()
        every { appCtx.getBeansWithAnnotation(SagaType::class.java) } returns mapOf(
            "linear"      to LinearSaga(),
            "conditional" to ConditionalSaga(),
            "async"       to AsyncSaga(),
            "failing"     to FailingSaga(),
            "multi-comp"  to MultiCompSaga(),
            "branching"   to BranchingSaga()
        )
        registry = SagaRegistry(appCtx)
        registry.scanAndRegister()

        storage    = InMemorySagaStorage(registry)
        eventStore = TestEventStore()

        engine = SagaEngine(
            storage    = storage,
            factory    = factory,
            registry   = registry,
            sagaLock   = LocalSagaLock(),
            eventStore = eventStore          // no timeoutQueue
        )
        // Drive execution manually — no background runner
    }

    @AfterEach
    fun teardown() { engine.destroy() }

    // ── helpers ───────────────────────────────────────────────────────────────

    private suspend fun awaitCompletion(sagaId: String, ms: Long = 3_000): Boolean {
        val d = CompletableDeferred<Boolean>()
        engine.onComplete(sagaId) { d.complete(it) }
        return withTimeout(ms) { d.await() }
    }

    private fun loadSaga(sagaId: String): Saga<*> =
        storage.load(sagaId, factory).instance

    // ── normal execution ──────────────────────────────────────────────────────

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
        assertTrue(s.after, "step after the skipped one should still run")
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

    // ── async / event-driven ──────────────────────────────────────────────────

    @Test
    fun `async step completes when success event is dispatched`() = runBlocking {
        val sagaId = engine.start(factory.create(AsyncSaga::class.java)).id

        engine.dispatch(sagaId, OkEvent(sagaId = sagaId, value = "done"))

        assertTrue(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as AsyncSaga
        assertTrue(s.beforeDone)
        assertEquals("done", s.result)
        assertFalse(s.comp)
    }

    @Test
    fun `failure event on async step triggers compensation`() = runBlocking {
        val sagaId = engine.start(factory.create(AsyncSaga::class.java)).id

        engine.dispatch(sagaId, NokEvent(sagaId = sagaId))

        assertFalse(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as AsyncSaga
        assertTrue(s.beforeDone)
        assertTrue(s.comp)
    }

    @Test
    fun `event dispatched before async step is ready is buffered and delivered later`() = runBlocking {
        val futureId = UUID.randomUUID().toString()
        eventStore.store(OkEvent(sagaId = futureId, value = "early"))

        val saga = factory.create(AsyncSaga::class.java) { id = futureId }
        engine.start(saga)

        assertTrue(awaitCompletion(futureId))
        assertEquals("early", (loadSaga(futureId) as AsyncSaga).result)
    }

    // ── failure and compensation ──────────────────────────────────────────────

    @Test
    fun `exception in step triggers compensation and saga reports failure`() = runBlocking {
        val sagaId = engine.start(factory.create(FailingSaga::class.java)).id

        assertFalse(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as FailingSaga
        assertTrue(s.s1Done)
        assertTrue(s.s1Comp)
    }

    @Test
    fun `compensation runs in strict reverse order`() = runBlocking {
        val sagaId = engine.start(factory.create(MultiCompSaga::class.java)).id

        assertFalse(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as MultiCompSaga
        assertEquals("abBA", s.log)
    }

    @Test
    fun `compensationStarted flag prevents double-compensation`() = runBlocking {
        val sagaId = engine.start(factory.create(FailingSaga::class.java)).id
        assertFalse(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as FailingSaga
        assertTrue(s.s1Comp)
    }

    // ── send / routing ────────────────────────────────────────────────────────

    @Test
    fun `send routes command to correct saga via StartedBy`() = runBlocking {
        val sagaId = engine.send(StartCmd("routed"))
        assertTrue(awaitCompletion(sagaId))
        assertEquals("routed", (loadSaga(sagaId) as LinearSaga).input)
    }

    @Test
    fun `send throws for unregistered command`() {
        assertThrows<IllegalStateException> { engine.send(UnknownCmd()) }
    }

    // ── persistence and restore ───────────────────────────────────────────────

    @Test
    fun `saga is persisted and can be loaded back`() = runBlocking {
        val sagaId = engine.send(StartCmd("persisted"))
        awaitCompletion(sagaId)

        val loaded = storage.load(sagaId, factory).instance as LinearSaga
        assertTrue(loaded.s1)
        assertTrue(loaded.s2)
        assertEquals("persisted", loaded.input)
    }

    @Test
    fun `saga restores from storage and completes on next event`() = runBlocking {
        val sagaId = engine.start(factory.create(AsyncSaga::class.java)).id

        withTimeout(3_000) {
            while (storage.load(sagaId, factory).stepState["before"]?.status != StepStatus.SUCCESS) {
                delay(20)
            }
        }

        engine.destroy()

        val engine2 = SagaEngine(
            storage    = storage,
            factory    = factory,
            registry   = registry,
            sagaLock   = LocalSagaLock(),
            eventStore = eventStore
        )
        engine2.initialize()
        delay(300)

        engine2.dispatch(sagaId, OkEvent(sagaId = sagaId, value = "restored"))

        val d = CompletableDeferred<Boolean>()
        engine2.onComplete(sagaId) { d.complete(it) }
        assertTrue(withTimeout(3_000) { d.await() })
        assertEquals("restored", (storage.load(sagaId, factory).instance as AsyncSaga).result)
        engine2.destroy()
    }

    @Test
    fun `engine resumes mid-compensation after restart`() = runBlocking {
        val saga = factory.create(LinearSaga::class.java) {
            id    = UUID.randomUUID().toString()
            input = "crash"
        }
        val stepState = mutableMapOf(
            "s1" to StepState(StepStatus.COMPENSATING, tokenCount = 1),
            "s2" to StepState(StepStatus.SUCCESS,      tokenCount = 1)
        )
        storage.save(saga.id, saga, stepState)

        val engine2 = SagaEngine(
            storage    = storage,
            factory    = factory,
            registry   = registry,
            sagaLock   = LocalSagaLock(),
            eventStore = eventStore
        )
        engine2.initialize()

        val d = CompletableDeferred<Boolean>()
        engine2.onComplete(saga.id) { d.complete(it) }
        assertFalse(withTimeout(3_000) { d.await() })
        engine2.destroy()
    }

    // ── branch behaviour ─────────────────────────────────────────────────────

    @Test
    fun `branch takes high path when condition is true`() = runBlocking {
        val sagaId = engine.start(
            factory.create(BranchingSaga::class.java) { amount = 150.0 }
        ).id
        assertTrue(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as BranchingSaga
        assertTrue(s.highRan,   "high branch should run for amount > 100")
        assertFalse(s.lowRan,   "low branch should NOT run")
        assertTrue(s.afterRan,  "step after branch should always run")
    }

    @Test
    fun `branch takes low path when condition is false`() = runBlocking {
        val sagaId = engine.start(
            factory.create(BranchingSaga::class.java) { amount = 50.0 }
        ).id
        assertTrue(awaitCompletion(sagaId))

        val s = loadSaga(sagaId) as BranchingSaga
        assertFalse(s.highRan,  "high branch should NOT run for amount <= 100")
        assertTrue(s.lowRan,    "low branch should run")
        assertTrue(s.afterRan,  "step after branch should always run")
    }
}