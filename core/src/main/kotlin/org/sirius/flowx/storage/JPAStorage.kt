package org.sirius.flowx.storage

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.persistence.*
import org.sirius.flowx.*
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

// -------------------- ENTITIES --------------------

// Regular class, not data class — Hibernate's proxy/dirty-checking relies on
// equals()/hashCode() semantics that data class breaks.

@Entity
@Table(name = "saga")
class SagaEntity(
    @Id
    val id: String,

    @Column(nullable = false)
    val type: String,

    @Column(nullable = false)
    var status: String,

    @Column(nullable = false, columnDefinition = "TEXT")
    var payload: String,

    @Column(nullable = false)
    val createdAt: Instant = Instant.now(),

    @Column(nullable = false)
    var updatedAt: Instant = Instant.now(),

    // EAGER so load() gets everything in one query.
    // CascadeType.ALL + orphanRemoval so a single sagaRepo.save() persists
    // all step changes — no stepRepo.save() calls needed.
    @OneToMany(
        mappedBy = "saga",
        cascade = [CascadeType.ALL],
        orphanRemoval = true,
        fetch = FetchType.EAGER
    )
    val steps: MutableList<SagaStepEntity> = mutableListOf()
)

@Entity
@Table(name = "saga_step")
class SagaStepEntity(
    @EmbeddedId
    val id: SagaStepId,

    @ManyToOne(fetch = FetchType.LAZY)
    @MapsId("sagaId")
    @JoinColumn(name = "saga_id")
    val saga: SagaEntity,

    @Column(nullable = false)
    var status: String,

    @Column(nullable = false)
    var tokenCount: Int = 0,

    @Column
    var timeoutAt: Long? = null,

    @Column(nullable = false)
    var updatedAt: Instant = Instant.now()
)

@Embeddable
data class SagaStepId(
    val sagaId: String = "",
    val stepId: String = ""
) : java.io.Serializable

// -------------------- REPOSITORIES --------------------

interface SagaEntityRepository : JpaRepository<SagaEntity, String> {
    // Include COMPENSATING so sagas that crashed mid-rollback are recovered on restart
    @Query("SELECT s.id FROM SagaEntity s WHERE s.status IN ('RUNNING', 'COMPENSATING')")
    fun findActiveIds(): List<String>
}

interface SagaStepEntityRepository : JpaRepository<SagaStepEntity, SagaStepId> {
    @Query("""
        SELECT DISTINCT s.id.sagaId FROM SagaStepEntity s
         WHERE s.status = 'AWAITING_EVENT'
           AND s.timeoutAt IS NOT NULL
           AND s.timeoutAt <= :now
    """)
    fun findTimedOutSagaIds(now: Long): List<String>
}

// -------------------- STORAGE IMPLEMENTATION --------------------

// @Transactional is declared here but SagaEngine must ensure storage calls
// never happen inside a suspended coroutine context — always wrap in
// withContext(Dispatchers.IO) in the engine so the transaction thread-local
// stays consistent for the full duration of the call.
@Transactional
class JpaSagaStorage(
    private val sagaRepo: SagaEntityRepository,
    private val stepRepo: SagaStepEntityRepository,
    private val registry: SagaRegistry
) : SagaStorage {

    private val mapper = jacksonObjectMapper()

    // -------------------- SAVE --------------------

    override fun save(sagaId: String, saga: Saga<*>, stepState: Map<String, StepState>) {
        val now     = Instant.now()
        val payload = mapper.writeValueAsString(buildPayload(saga))
        val status  = deriveSagaStatus(stepState)

        // Load existing entity or create a new one
        val entity = sagaRepo.findById(sagaId).orElse(null)
            ?: SagaEntity(
                id        = sagaId,
                type      = sagaTypeName(saga),
                status    = status,
                payload   = payload,
                createdAt = now,
                updatedAt = now
            )

        entity.status    = status
        entity.payload   = payload
        entity.updatedAt = now

        // Upsert each step via the in-memory list — CascadeType.ALL means
        // a single sagaRepo.save() at the end writes everything.
        // This avoids N+1 stepRepo.findById() calls.
        stepState.forEach { (stepId, state) ->
            val stepEntity = entity.steps.find { it.id.stepId == stepId }
                ?: SagaStepEntity(
                    id     = SagaStepId(sagaId = sagaId, stepId = stepId),
                    saga   = entity,
                    status = state.status.name
                ).also { entity.steps.add(it) }

            stepEntity.status     = state.status.name
            stepEntity.tokenCount = state.tokenCount
            stepEntity.timeoutAt  = state.timeoutAt
            stepEntity.updatedAt  = now
        }

        sagaRepo.save(entity)
    }

    // -------------------- LOAD --------------------

    @Transactional(readOnly = true)
    override fun load(sagaId: String, factory: SagaFactory): LoadedSaga<Saga<*>> {
        val entity = sagaRepo.findById(sagaId).orElseThrow {
            IllegalStateException("Saga not found: $sagaId")
        }

        val payload: Map<String, Any?> =
            mapper.readValue(entity.payload, Map::class.java) as Map<String, Any?>

        val clazz    = registry.get(entity.type) as Class<Saga<*>>
        val instance = factory.create(clazz) {
            val descriptor = SagaDescriptorCache.get(clazz)
            descriptor.persistedFields.forEach { field ->
                field.set(this, payload[field.name])
            }
        }

        val stepStateMap = entity.steps.associate { step ->
            step.id.stepId to StepState(
                status     = StepStatus.valueOf(step.status),
                tokenCount = step.tokenCount,
                timeoutAt  = step.timeoutAt
            )
        }.toMutableMap()

        return LoadedSaga(instance, stepStateMap)
    }

    // -------------------- QUERIES --------------------

    @Transactional(readOnly = true)
    override fun findActiveSagaIds(): List<String> =
        sagaRepo.findActiveIds()

    // -------------------- HELPERS --------------------

    private fun buildPayload(saga: Saga<*>): Map<String, Any?> {
        val descriptor = SagaDescriptorCache.get(saga::class.java)
        return descriptor.persistedFields.associate { it.name to it.get(saga) }
    }

    private fun sagaTypeName(saga: Saga<*>): String =
        saga::class.java.getAnnotation(SagaType::class.java)?.name
            ?: error("Saga class ${saga::class.simpleName} has no @SagaType annotation")

    private fun deriveSagaStatus(stepState: Map<String, StepState>): String = when {
        // COMPENSATING takes priority: a crash mid-compensation leaves the
        // saga in this state so findActiveSagaIds can recover it on restart.
        stepState.values.any { it.status == StepStatus.COMPENSATING } -> "COMPENSATING"
        stepState.values.any { it.status == StepStatus.FAILED }       -> "FAILED"
        stepState.values.any { it.status == StepStatus.COMPENSATED }  -> "COMPENSATED"
        stepState.values.all { it.status == StepStatus.SUCCESS }      -> "COMPLETED"
        else                                                           -> "RUNNING"
    }
}