package org.sirius.flowx
/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Index
import jakarta.persistence.LockModeType
import jakarta.persistence.QueryHint
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.jpa.repository.QueryHints
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

/**
 * A single event row that has been atomically claimed by this engine instance.
 * [eventClass] is the fully-qualified class name so we can deserialise back to
 * the right concrete Event subtype.
 */
data class ClaimedEvent(
    val id: String,
    val sagaId: String,
    val eventClass: String,
    val payload: String
) {
    fun deserialize(mapper: ObjectMapper = jacksonObjectMapper()): Event =
        mapper.readValue(payload, Class.forName(eventClass)) as Event
}

/**
 * Cluster-safe persistent event queue.
 *
 * Implementations MUST guarantee that [claimPending] is atomic (e.g. via
 * `SELECT … FOR UPDATE SKIP LOCKED` on PostgreSQL / MySQL) so that two
 * competing engine instances never process the same event.
 *
 * Lifecycle of a row:
 *   PENDING → (claimed by one node) → CLAIMED → PROCESSED
 *                                              ↘ FAILED
 * A periodic stale-claim recovery resets CLAIMED rows that have been held
 * longer than [releaseStaleClaims]'s threshold back to PENDING so that a
 * replacement node can pick them up after a crash.
 */
interface SagaEventStore {

    /** Persist an inbound event before any delivery attempt. */
    fun store(event: Event)

    /**
     * Atomically claim up to [limit] PENDING events for [sagaId] and tag them
     * with [instanceId].  The transaction must commit before this call returns
     * so the lock is visible to all peers immediately.
     */
    fun claimPending(
        sagaId: String,
        instanceId: String,
        limit: Int = 10
    ): List<ClaimedEvent>

    /** Mark a previously claimed event as successfully delivered. */
    fun markProcessed(eventId: String)

    /**
     * Release a claim without processing — resets the row to PENDING so
     * another node (or the same node on the next tick) can retry.
     */
    fun releaseClaim(eventId: String)

    /** Mark a claimed event as permanently failed (bad payload, missing handler, …). */
    fun markFailed(eventId: String, reason: String? = null)

    /**
     * Reset CLAIMED rows whose [claimedAt] is older than [olderThanMs]
     * milliseconds back to PENDING.  Call periodically to recover from
     * node crashes that happened between claim and markProcessed.
     */
    fun releaseStaleClaims(olderThanMs: Long = 30_000)
}


// ──────────────────────────────────────────────────────────────────────────────
// Entity
// ──────────────────────────────────────────────────────────────────────────────

/**
 * One inbound saga event.
 *
 * Status lifecycle:
 *   PENDING  – stored, not yet claimed
 *   CLAIMED  – atomically locked by one engine instance
 *   PROCESSED – delivered successfully
 *   FAILED   – unrecoverable delivery error
 *
 * Indexes cover the two hot query paths:
 *   1. claim query  → (saga_id, status)
 *   2. stale-claim recovery → (status, claimed_at)
 */
@Entity
@Table(
    name = "saga_event",
    indexes = [
        Index(name = "idx_saga_event_saga_status",   columnList = "saga_id, status"),
        Index(name = "idx_saga_event_status_claimed", columnList = "status, claimed_at")
    ]
)
class SagaEventEntity(

    @Id
    @Column(nullable = false, length = 36)
    val id: String,

    @Column(name = "saga_id", nullable = false, length = 36)
    val sagaId: String,

    /** Fully-qualified class name of the event (used for deserialisation). */
    @Column(name = "event_class", nullable = false)
    val eventClass: String,

    /** Jackson-serialised event payload. */
    @Column(nullable = false, columnDefinition = "TEXT")
    val payload: String,

    @Column(nullable = false, length = 16)
    var status: String = "PENDING",

    @Column(name = "created_at", nullable = false)
    val createdAt: Instant = Instant.now(),

    /** Timestamp when this row was locked by an engine instance. */
    @Column(name = "claimed_at")
    var claimedAt: Instant? = null,

    /** Identifies which engine instance holds the claim. */
    @Column(name = "claimed_by", length = 64)
    var claimedBy: String? = null,

    @Column(name = "processed_at")
    var processedAt: Instant? = null,

    @Column(name = "error_message", columnDefinition = "TEXT")
    var errorMessage: String? = null
)

// ──────────────────────────────────────────────────────────────────────────────
// Repository
// ──────────────────────────────────────────────────────────────────────────────

interface SagaEventEntityRepository : JpaRepository<SagaEventEntity, String> {

    /**
     * Atomically lock up to [limit] PENDING rows for [sagaId].
     *
     * `FOR UPDATE SKIP LOCKED` is supported by PostgreSQL 9.5+ and MySQL 8+.
     * Rows already locked by a peer are silently skipped — only this instance
     * gets a unique, non-overlapping slice of work.
     *
     * The transaction that calls this query MUST commit before the caller
     * processes the events (see [JpaSagaEventStore.claimPending]).
     */
    @org.springframework.data.jpa.repository.Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(
        QueryHint(
            name  = "jakarta.persistence.lock.timeout",
            value = "-2"   // -2 == LockOptions.SKIP_LOCKED in Hibernate 6
        )
    )
    @Query("""
    SELECT e FROM SagaEventEntity e
     WHERE e.sagaId = :sagaId
       AND e.status = 'PENDING'
     ORDER BY e.createdAt ASC
     LIMIT :limit
""")
    fun lockPending(
        @Param("sagaId") sagaId: String,
        @Param("limit")  limit: Int
    ): List<SagaEventEntity>

    /**
     * Find CLAIMED rows whose claim is older than [threshold].
     * Used by stale-claim recovery to reset crashed-node work back to PENDING.
     */
    @Query(
        value = """
            SELECT *
              FROM saga_event
             WHERE status     = 'CLAIMED'
               AND claimed_at < :threshold
        """,
        nativeQuery = true
    )
    fun findStaleClaims(
        @Param("threshold") threshold: Instant
    ): List<SagaEventEntity>

    /**
     * Bulk-update to PENDING — used by stale-claim recovery.
     * A native bulk UPDATE is faster than load-then-saveAll for large batches.
     */
    @Modifying
    @Query(
        value = """
            UPDATE saga_event
               SET status     = 'PENDING',
                   claimed_at  = NULL,
                   claimed_by  = NULL
             WHERE status     = 'CLAIMED'
               AND claimed_at < :threshold
        """,
        nativeQuery = true
    )
    fun releaseStale(@Param("threshold") threshold: Instant): Int
}


/**
 * PostgreSQL / MySQL-backed [SagaEventStore].
 *
 * ### Cluster-safety guarantee
 * [claimPending] runs `SELECT … FOR UPDATE SKIP LOCKED` inside its own
 * `REQUIRES_NEW` transaction and commits before returning.  This means:
 *
 *   • Two nodes calling [claimPending] concurrently for the same [sagaId]
 *     receive non-overlapping sets of events — no duplicate delivery.
 *   • The claim is durable; a node crash after claiming but before
 *     [markProcessed] leaves rows in CLAIMED state, which the stale-claim
 *     recovery job resets to PENDING so another node can retry.
 *
 * ### Why REQUIRES_NEW in claimPending?
 * The engine calls this method from a coroutine dispatched on Dispatchers.IO.
 * Using REQUIRES_NEW ensures the lock is held only for the brief
 * claim+update window and is released (committed) before the caller starts
 * the potentially-slow event delivery, keeping row contention minimal.
 */
open class JpaSagaEventStore(
    private val repo: SagaEventEntityRepository
) : SagaEventStore {

    private val mapper = jacksonObjectMapper()

    // ──────────────────────────────────────────────────────────────────────
    // Store
    // ──────────────────────────────────────────────────────────────────────

    @Transactional
    override open fun store(event: Event) {
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

    // ──────────────────────────────────────────────────────────────────────
    // Claim  (REQUIRES_NEW so the lock commits before delivery starts)
    // ──────────────────────────────────────────────────────────────────────

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    override fun claimPending(
        sagaId: String,
        instanceId: String,
        limit: Int
    ): List<ClaimedEvent> {

        val rows = repo.lockPending(sagaId, limit)
        if (rows.isEmpty()) return emptyList()

        val now = Instant.now()
        rows.forEach { row ->
            row.status    = "CLAIMED"
            row.claimedAt = now
            row.claimedBy = instanceId
        }
        repo.saveAll(rows)   // flush inside REQUIRES_NEW → committed on return

        return rows.map { row ->
            ClaimedEvent(
                id         = row.id,
                sagaId     = row.sagaId,
                eventClass = row.eventClass,
                payload    = row.payload
            )
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Post-delivery state transitions
    // ──────────────────────────────────────────────────────────────────────

    @Transactional
    override fun markProcessed(eventId: String) {
        repo.findById(eventId).ifPresent { row ->
            row.status      = "PROCESSED"
            row.processedAt = Instant.now()
            repo.save(row)
        }
    }

    @Transactional
    override fun releaseClaim(eventId: String) {
        repo.findById(eventId).ifPresent { row ->
            row.status    = "PENDING"
            row.claimedAt = null
            row.claimedBy = null
            repo.save(row)
        }
    }

    @Transactional
    override fun markFailed(eventId: String, reason: String?) {
        repo.findById(eventId).ifPresent { row ->
            row.status       = "FAILED"
            row.errorMessage = reason
            repo.save(row)
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Stale-claim recovery
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Bulk-resets CLAIMED rows whose claim is older than [olderThanMs]
     * milliseconds back to PENDING.
     *
     * Call this periodically (e.g. every 30 s in the runner loop) so that
     * events claimed by a crashed node are eventually re-delivered by a
     * surviving peer.
     */
    @Transactional
    override fun releaseStaleClaims(olderThanMs: Long) {
        val threshold = Instant.now().minusMillis(olderThanMs)
        val released  = repo.releaseStale(threshold)
        if (released > 0) {
            // logged by the caller (SagaEngine has the logger)
        }
    }
}
