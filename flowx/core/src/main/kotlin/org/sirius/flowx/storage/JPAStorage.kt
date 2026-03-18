package org.sirius.flowx.storage

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.persistence.*
import org.sirius.flowx.*
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

// ── Entities ──────────────────────────────────────────────────────────────────

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

    /**
     * Stamped on every persist() call.
     * Used by recoverStuckSagas() to prioritise sagas that haven't been
     * touched the longest — naturally floats orphans and starved sagas to
     * the top of the recovery scan. NULL = never processed, highest priority.
     */
    @Column(name = "last_processed_at")
    var lastProcessedAt: Instant? = null,

    @OneToMany(
        mappedBy      = "saga",
        cascade       = [CascadeType.ALL],
        orphanRemoval = true,
        fetch         = FetchType.EAGER
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

// ── Repositories ──────────────────────────────────────────────────────────────

interface SagaEntityRepository : JpaRepository<SagaEntity, String> {

    @Query("SELECT s.id FROM SagaEntity s WHERE s.status IN ('RUNNING', 'COMPENSATING')")
    fun findActiveIds(): List<String>

    /**
     * Active sagas sorted by lastProcessedAt ASC NULLS FIRST.
     * Sagas never touched (NULL) or touched longest ago come first —
     * naturally prioritises orphans from crashed nodes and starved sagas.
     * Excludes AWAITING_EVENT — those are handled by findAwaitingWithPendingEvents.
     */
    @Query("""
        SELECT s.id FROM SagaEntity s
         WHERE s.status IN ('RUNNING', 'COMPENSATING')
         ORDER BY s.lastProcessedAt ASC NULLS FIRST
         LIMIT :limit
    """)
    fun findLeastRecentlyProcessed(@Param("limit") limit: Int): List<String>

    /**
     * AWAITING_EVENT sagas that have PENDING events sitting in the event store.
     * These represent dropped work — a node stored the event but crashed before
     * draining it. Sorted by event creation time so oldest unprocessed events
     * are recovered first.
     */
    @Query("""
    SELECT s.id FROM SagaEntity s
     WHERE s.status = 'RUNNING'
       AND s.id IN (
           SELECT e.sagaId FROM SagaEventEntity e
            WHERE e.status = 'PENDING'
       )
     ORDER BY s.lastProcessedAt ASC NULLS FIRST
     LIMIT :limit
""")
    fun findAwaitingWithPendingEvents(@Param("limit") limit: Int): List<String>
}

// ── Storage Implementation ────────────────────────────────────────────────────

@Transactional
class JpaSagaStorage(
    private val sagaRepo: SagaEntityRepository,
    private val registry: SagaRegistry
) : SagaStorage {

    private val mapper = jacksonObjectMapper()

    override fun save(sagaId: String, saga: Saga<*>, stepState: Map<String, StepState>) {
        val now     = Instant.now()
        val payload = mapper.writeValueAsString(buildPayload(saga))
        val status  = deriveSagaStatus(stepState)

        val entity = sagaRepo.findById(sagaId).orElse(null)
            ?: SagaEntity(
                id        = sagaId,
                type      = sagaTypeName(saga),
                status    = status,
                payload   = payload,
                createdAt = now,
                updatedAt = now
            )

        entity.status          = status
        entity.payload         = payload
        entity.updatedAt       = now
        entity.lastProcessedAt = now    // stamp every persist

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

    @Transactional(readOnly = true)
    override fun findActiveSagaIds(): List<String> = sagaRepo.findActiveIds()

    @Transactional(readOnly = true)
    override fun findLeastRecentlyProcessed(limit: Int): List<String> =
        sagaRepo.findLeastRecentlyProcessed(limit)

    @Transactional(readOnly = true)
    override fun findAwaitingWithPendingEvents(limit: Int): List<String> =
        sagaRepo.findAwaitingWithPendingEvents(limit)

    private fun buildPayload(saga: Saga<*>): Map<String, Any?> {
        val descriptor = SagaDescriptorCache.get(saga::class.java)
        return descriptor.persistedFields.associate { it.name to it.get(saga) }
    }

    private fun sagaTypeName(saga: Saga<*>): String =
        saga::class.java.getAnnotation(SagaType::class.java)?.name
            ?: error("Saga class ${saga::class.simpleName} has no @SagaType annotation")

    private fun deriveSagaStatus(stepState: Map<String, StepState>): String = when {
        stepState.values.any { it.status == StepStatus.COMPENSATING }  -> "COMPENSATING"
        stepState.values.any { it.status == StepStatus.FAILED }        -> "FAILED"
        stepState.values.any { it.status == StepStatus.COMPENSATED }   -> "COMPENSATED"
        stepState.values.all {
            it.status == StepStatus.SUCCESS || it.status == StepStatus.SKIPPED
        }                                                               -> "COMPLETED"
        else                                                            -> "RUNNING"
    }
}