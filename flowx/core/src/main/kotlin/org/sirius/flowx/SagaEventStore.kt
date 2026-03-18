package org.sirius.flowx

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.persistence.*
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

data class PendingEvent(
    val id: String,
    val sagaId: String,
    val eventClass: String,
    val payload: String
) {
    fun deserialize(mapper: ObjectMapper = jacksonObjectMapper()): Event =
        mapper.readValue(payload, Class.forName(eventClass)) as Event
}

interface SagaEventStore {
    /** Persist an inbound event before delivery. */
    fun store(event: Event)

    /** Fetch all PENDING events for this saga — no locking needed, sagaLock serialises access. */
    fun getPending(sagaId: String): List<PendingEvent>

    /** Mark successfully delivered. */
    fun markProcessed(eventId: String)

    /** Mark permanently failed. */
    fun markFailed(eventId: String, reason: String? = null)
}

// ── Entity ────────────────────────────────────────────────────────────────────

@Entity
@Table(
    name = "saga_event",
    indexes = [
        Index(name = "idx_saga_event_saga_status", columnList = "saga_id, status")
    ]
)
class SagaEventEntity(

    @Id
    @Column(nullable = false, length = 36)
    val id: String,

    @Column(name = "saga_id", nullable = false, length = 36)
    val sagaId: String,

    @Column(name = "event_class", nullable = false)
    val eventClass: String,

    @Column(nullable = false, columnDefinition = "TEXT")
    val payload: String,

    @Column(nullable = false, length = 16)
    var status: String = "PENDING",

    @Column(name = "created_at", nullable = false)
    val createdAt: Instant = Instant.now(),

    @Column(name = "processed_at")
    var processedAt: Instant? = null,

    @Column(name = "error_message", columnDefinition = "TEXT")
    var errorMessage: String? = null
)

// ── Repository ────────────────────────────────────────────────────────────────

interface SagaEventEntityRepository : JpaRepository<SagaEventEntity, String> {

    @Query("""
        SELECT e FROM SagaEventEntity e
         WHERE e.sagaId = :sagaId
           AND e.status = 'PENDING'
         ORDER BY e.createdAt ASC
    """)
    fun findPending(@Param("sagaId") sagaId: String): List<SagaEventEntity>

    @Modifying
    @Query("""
        UPDATE SagaEventEntity e
           SET e.status = 'PROCESSED',
               e.processedAt = :now
         WHERE e.id = :id
    """)
    fun markProcessed(@Param("id") id: String, @Param("now") now: Instant)

    @Modifying
    @Query("""
        UPDATE SagaEventEntity e
           SET e.status = 'FAILED',
               e.errorMessage = :reason
         WHERE e.id = :id
    """)
    fun markFailed(@Param("id") id: String, @Param("reason") reason: String?)
}

// ── Implementation ────────────────────────────────────────────────────────────

open class JpaSagaEventStore(
    private val repo: SagaEventEntityRepository
) : SagaEventStore {

    private val mapper = jacksonObjectMapper()

    @Transactional
    override fun store(event: Event) {
        repo.save(
            SagaEventEntity(
                id         = UUID.randomUUID().toString(),
                sagaId     = event.sagaId,
                eventClass = event::class.java.name,
                payload    = mapper.writeValueAsString(event),
                status     = "PENDING",
                createdAt  = Instant.now()
            )
        )
    }

    @Transactional(readOnly = true)
    override fun getPending(sagaId: String): List<PendingEvent> =
        repo.findPending(sagaId).map { row ->
            PendingEvent(
                id         = row.id,
                sagaId     = row.sagaId,
                eventClass = row.eventClass,
                payload    = row.payload
            )
        }

    @Transactional
    override fun markProcessed(eventId: String) {
        repo.markProcessed(eventId, Instant.now())
    }

    @Transactional
    override fun markFailed(eventId: String, reason: String?) {
        repo.markFailed(eventId, reason)
    }
}