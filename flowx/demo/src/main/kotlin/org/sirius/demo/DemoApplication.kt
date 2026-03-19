package org.sirius.demo

import jakarta.annotation.PostConstruct
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.sirius.demo.PaymentConfirmed
import org.sirius.flowx.AbstractSaga
import org.sirius.flowx.Event
import org.sirius.flowx.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Import
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.function.Consumer
import kotlin.reflect.KClass


/*TODO @Component
class SagaEngineAutoStarter(
    private val runner: SagaEngineRunner
) {

    @EventListener(ApplicationReadyEvent::class)
    fun onStartup() {
        runner.start()
    }
}*/

@Component
class EventBus {

    private val subscribers =
        ConcurrentHashMap<KClass<*>, MutableList<suspend (Any) -> Unit>>()

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun <T : Any> subscribe(eventType: KClass<T>, handler: suspend (T) -> Unit) {
        val handlers = subscribers.computeIfAbsent(eventType) {
            CopyOnWriteArrayList()
        }

        handlers.add { event -> handler(event as T) }
    }

    fun publish(event: Any) {
        val handlers = subscribers[event::class] ?: return

        for (handler in handlers) {
            scope.launch {
                try {
                    handler(event)
                } catch (e: Exception) {
                    println("Event handler failed: ${e.message}")
                }
            }
        }
    }
}

// -------------------- COMMANDS --------------------

data class PlaceOrderCommand(
    val customerName: String,
    val amount: Double
) : Command

// -------------------- EVENTS --------------------

data class EventEnvelope<T>(
    val sagaId: String,
    val event: T
)

data class PaymentConfirmed(override val sagaId: String, val txId: String)  : Event
data class PaymentFailed(override val sagaId: String, val reason: String)   : Event
data class ApprovalGranted(override val sagaId: String)                     : Event
data class ApprovalDenied(override val sagaId: String, val reason: String)  : Event

// -------------------- SERVICES --------------------

@Component
class PaymentService(val bus : EventBus) {
    fun pay(sagaId: String, amount: Double) {
        println("PaymentService.pay($amount)")

        bus.publish(PaymentConfirmed(sagaId = sagaId, txId = "TX-001"))
    }

    fun compensate()        = println("PaymentService.compensate")
}

@Component
class InventoryService(val bus : EventBus) {
    fun reserve()    = println("InventoryService.reserve")
    fun compensate() = println("InventoryService.compensate")
}

@Component
class ApprovalService(val bus : EventBus) {
    fun requestApproval(sagaId: String, customerName: String) {
        println("ApprovalService.requestApproval($customerName)")

        bus.publish(ApprovalGranted(sagaId = sagaId))
    }
}

// -------------------- SAGA --------------------

@SagaType("order")
@StartedBy(PlaceOrderCommand::class)
class OrderSaga : AbstractSaga<OrderSaga>() {

    @Persisted var customerName: String = ""
    @Persisted var amount: Double = 0.0
    @Persisted var txId: String = ""

    @Autowired lateinit var paymentService: PaymentService
    @Autowired lateinit var inventoryService: InventoryService
    @Autowired lateinit var approvalService: ApprovalService

    override fun definition(): SagaDefinition<OrderSaga> = definition

    companion object {
        val definition: SagaDefinition<OrderSaga> = saga {

            // Populate saga state from the triggering command —
            // saga fields are directly in scope (no 'saga.' prefix needed)

            on<PlaceOrderCommand> { cmd ->
                customerName = cmd.customerName
                amount       = cmd.amount
            }

            step("validate") {
                execute   {
                    println("validate order for $customerName ($$amount)")
                }
                compensate { println("compensate validate") }
            }

            // branch: high-value orders need manual approval, others skip straight through
            branch {
                on({ amount > 1000.0 }) {
                    step("manualApproval") {
                        execute {
                            approvalService.requestApproval(id, customerName)
                        }
                        compensate { println("compensate manualApproval") }

                        // Receiver IS the saga — access fields directly, no 'saga' param
                        onSuccess<ApprovalGranted> { _ ->
                            println("approval granted for $customerName")
                        }
                        onFailure<ApprovalDenied> { event ->
                            println("approval denied: ${event.reason}")
                        }

                        timeout(60_000)
                    }
                }
                otherwise {
                    // Low-value order: auto-approved, nothing to do
                    step("autoApprove") {
                        execute {
                            println("auto-approved for $customerName")
                        }
                    }
                }
            }

            parallel(join = Join.ALL) {
                step("payment") {
                    execute {
                        paymentService.pay(id, amount)
                    }
                    compensate { paymentService.compensate() }

                    // Receiver IS the saga — txId is a saga field, set it directly
                    onSuccess<PaymentConfirmed> { event ->
                        txId = event.txId
                        println("payment confirmed txId=$txId")
                    }
                    onFailure<PaymentFailed> { event ->
                        println("payment failed: ${event.reason}")
                    }

                    timeout(30_000)
                }

                step("inventory") {
                    execute   {
                        inventoryService.reserve()
                    }
                    compensate { inventoryService.compensate() }

                    // Only reserve for substantial amounts — step-level condition
                    condition { amount > 0.0 }
                }
            }

            step("shipping") {
                execute {
                    println("shipping order for $customerName (txId=$txId)")
                    // Uncomment to test compensation:
                    // throw RuntimeException("shipping service unavailable")
                }
            }
        }
    }
}

// -------------------- APPLICATION --------------------

@SpringBootApplication(
    exclude = [
        org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration::class,
        org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration::class,           // ← add
        org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration::class
    ] // ← add
)
@Import(FlowxConfiguration::class)
class DemoApplication

// -------------------- RUNNER --------------------

@Component
class Runner(val engine: SagaEngine, val runner: SagaEngineRunner, val eventBus : EventBus) {

    @PostConstruct
    fun run() {
        // Start the engine runner manually since ApplicationReadyEvent
        // won't fire until after @PostConstruct returns
        runner.start()

        EventDispatcher.register(Event::class) { event -> event.sagaId to event }

        eventBus.subscribe<ApprovalGranted>(ApprovalGranted::class)   { event -> EventDispatcher.dispatch(event) }
        eventBus.subscribe<ApprovalDenied>(ApprovalDenied::class)     { event -> EventDispatcher.dispatch(event) }
        eventBus.subscribe<PaymentConfirmed>(PaymentConfirmed::class) { event -> EventDispatcher.dispatch(event) }

        val latch = java.util.concurrent.CountDownLatch(1)

        val sagaId = engine.send(PlaceOrderCommand(customerName = "Andreas", amount = 1499.99))
        engine.onComplete(sagaId) { success ->
            println(if (success) "Order saga completed!" else "Order saga failed/compensated")
            latch.countDown()
        }

        /*val sagaId2 = engine.send(PlaceOrderCommand(customerName = "Bob", amount = 49.99))
        engine.onComplete(sagaId2) { success ->
            println(if (success) "Small order completed!" else "Small order failed")
            latch.countDown()
        }*/

        latch.await()
        println("All sagas done.")
    }
}

// -------------------- MAIN --------------------

fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}