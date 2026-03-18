package org.sirius.flowx

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

// -------------------- STORAGE CONTRACT --------------------

interface SagaStorage {
    fun save(sagaId: String, instance: Saga<*>, stepState: Map<String, StepState>)
    fun load(sagaId: String, factory: SagaFactory): LoadedSaga<Saga<*>>
    fun findActiveSagaIds(): List<String>
}

class LoadedSaga<T : Saga<*>>(
    val instance: T,
    val stepState: MutableMap<String, StepState>,
    val completion: CompletableDeferred<Boolean> = CompletableDeferred(),
    // Per-saga mutex — coroutine-safe replacement for synchronized(runtime)
    val mutex: Mutex = Mutex(),
    // Guards against compensate() being triggered more than once (e.g. execute
    // failure racing with the timeout scanner).
    // AtomicBoolean so compareAndSet is truly atomic — @Volatile Boolean is not.
    val compensationStarted: AtomicBoolean = AtomicBoolean(false)
)

// -------------------- IN-MEMORY JSON IMPLEMENTATION --------------------

class InMemorySagaStorage(private val registry: SagaRegistry) : SagaStorage {
    private val mapper = jacksonObjectMapper()
    // ConcurrentHashMap so concurrent save/load from multiple saga coroutines is safe
    private val storage = ConcurrentHashMap<String, String>()

    override fun save(sagaId: String, saga: Saga<*>, stepState: Map<String, StepState>) {
        val persisted = mutableMapOf<String, Any?>()
        val descriptor = SagaDescriptorCache.get(saga::class.java)
        descriptor.persistedFields.forEach { field ->
            persisted[field.name] = field.get(saga)
        }
        persisted["__sagaType"] = saga::class.java.getAnnotation(SagaType::class.java)?.name

        val wrapper = mapOf(
            "id" to sagaId,
            "stepState" to stepState,
            "persisted" to persisted
        )
        storage[sagaId] = mapper.writeValueAsString(wrapper)
    }

    override fun load(sagaId: String, factory: SagaFactory): LoadedSaga<Saga<*>> {
        val json = storage[sagaId] ?: throw IllegalStateException("Saga not found: $sagaId")
        val wrapper: Map<String, Any?> = mapper.readValue(json, Map::class.java) as Map<String, Any?>
        val persisted = wrapper["persisted"] as Map<String, Any?>

        val sagaTypeName = persisted["__sagaType"] as? String
            ?: throw IllegalStateException("Missing saga type for $sagaId")
        val clazz = registry.get(sagaTypeName) as Class<Saga<*>>

        val saga = factory.create(clazz) {
            val descriptor = SagaDescriptorCache.get(clazz)
            descriptor.persistedFields.forEach { field ->
                field.set(this, persisted[field.name])
            }
        }

        val stepStateMap =
            (wrapper["stepState"] as? Map<String, Map<String, Any?>>)?.mapValues { (_, v) ->
                StepState(
                    status = StepStatus.valueOf(v["status"].toString()),
                    tokenCount = (v["tokenCount"] as Number).toInt(),
                    timeoutAt = (v["timeoutAt"] as? Number)?.toLong()
                )
            }?.toMutableMap() ?: mutableMapOf()

        return LoadedSaga(saga, stepStateMap)
    }

    override fun findActiveSagaIds(): List<String> {
        val activeStatuses = setOf(
            StepStatus.RUNNING.name,
            StepStatus.COMPENSATING.name,
            StepStatus.AWAITING_EVENT.name,
            StepStatus.PENDING.name
        )
        return storage.filter { (_, json) ->
            val wrapper: Map<String, Any?> = mapper.readValue(json, Map::class.java) as Map<String, Any?>
            val stepState = wrapper["stepState"] as Map<String, Map<String, Any?>>
            stepState.values.any { it["status"] in activeStatuses }
        }.keys.toList()
    }
}