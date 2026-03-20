// ─────────────────────────────────────────────────────────────────────────────
// build.gradle.kts — add these two dependencies
// ─────────────────────────────────────────────────────────────────────────────

// implementation("org.springframework.boot:spring-boot-starter-actuator")
// implementation("io.micrometer:micrometer-registry-prometheus")


// ─────────────────────────────────────────────────────────────────────────────
// RpcMetrics.kt
//
// Drop this bean into your ServiceNode Spring context.
// It instruments every RPC method call automatically and also
// exposes helpers for emitting ad-hoc metrics from your own code.
// ─────────────────────────────────────────────────────────────────────────────

package org.sirius.dynamicrpc

import io.micrometer.core.instrument.*
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class RpcMetrics(private val registry: MeterRegistry) {

    // ── Counters ──────────────────────────────────────────────────────────────

    /** Total RPC calls, tagged by service + method */
    fun incrementCall(service: String, method: String) =
        Counter.builder("rpc.calls.total")
            .description("Total number of RPC calls")
            .tag("service", service)
            .tag("method",  method)
            .register(registry)
            .increment()

    /** Total RPC errors, tagged by service + method + exception type */
    fun incrementError(service: String, method: String, ex: Throwable) =
        Counter.builder("rpc.errors.total")
            .description("Total number of RPC errors")
            .tag("service",   service)
            .tag("method",    method)
            .tag("exception", ex::class.simpleName ?: "Unknown")
            .register(registry)
            .increment()

    // ── Timers ────────────────────────────────────────────────────────────────

    /**
     * Wrap a suspend block and record its wall-clock duration.
     *
     * Usage:
     *   val result = metrics.recordDuration("UserService", "getUser") {
     *       userService.getUser(id)
     *   }
     */
    suspend fun <T> recordDuration(service: String, method: String, block: suspend () -> T): T {
        val sample = Timer.start(registry)
        return try {
            block().also {
                sample.stop(
                    Timer.builder("rpc.duration.seconds")
                        .description("RPC call duration")
                        .tag("service", service)
                        .tag("method",  method)
                        .tag("outcome", "success")
                        .register(registry)
                )
            }
        } catch (ex: Throwable) {
            sample.stop(
                Timer.builder("rpc.duration.seconds")
                    .tag("service", service)
                    .tag("method",  method)
                    .tag("outcome", "error")
                    .register(registry)
            )
            throw ex
        }
    }

    // ── Gauges ────────────────────────────────────────────────────────────────

    private val activeCalls = AtomicInteger(0)

    init {
        // Gauge stays alive as long as the bean does
        Gauge.builder("rpc.calls.active", activeCalls) { it.get().toDouble() }
            .description("Number of RPC calls currently in flight")
            .register(registry)
    }

    fun trackActive(block: () -> Unit) {
        activeCalls.incrementAndGet()
        try { block() } finally { activeCalls.decrementAndGet() }
    }
}


// ─────────────────────────────────────────────────────────────────────────────
// Registry.kt — instrument the handler in scanAndRegister()
//
// Replace the handler lambda in service.methods[methodName] = ServiceMethod(...)
// ─────────────────────────────────────────────────────────────────────────────

/*
// Inject RpcMetrics into Registry — or pass it as a parameter to scanAndRegister()

service.methods[methodName] = ServiceMethod(
    name    = methodName,
    handler = { request ->
        metrics.incrementCall(serviceName, methodName)
        metrics.recordDuration(serviceName, methodName) {
            val args = funcParams.map { param ->
                coerceArg(params[param.name!!], param.type.classifier as KClass<*>)
            }
            implFunc.call(bean, *args.toTypedArray())
        }
    }
)
*/


// ─────────────────────────────────────────────────────────────────────────────
// Ad-hoc usage anywhere in your own service code
// ─────────────────────────────────────────────────────────────────────────────

/*
@Component
class UserServiceImpl(
    private val metrics: RpcMetrics,
    private val meterRegistry: MeterRegistry   // raw registry for one-off metrics
) : UserService {

    override fun getUser(id: Int): User {
        // Simple counter
        metrics.incrementCall("UserService", "getUser")

        // One-off gauge — e.g. track DB pool size, queue depth, etc.
        Gauge.builder("userservice.last.requested.id", id) { it.toDouble() }
            .register(meterRegistry)

        return User(id, "User-$id")
    }

    override fun greet(name: String): String {
        // Distribution summary — e.g. request payload size
        DistributionSummary.builder("userservice.name.length")
            .baseUnit("chars")
            .register(meterRegistry)
            .record(name.length.toDouble())

        return "Hello, $name!"
    }
}
*/