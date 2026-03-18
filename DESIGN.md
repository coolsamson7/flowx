# FlowX Saga Engine — Architecture & Design

## Execution Model

The engine is fully event-driven. Once a saga is started, all progress is driven
by coroutines reacting to events — there is no polling loop driving normal execution.
The runner loop exists only for crash recovery, not for primary execution.

### Happy path flow

```
send(command)
  → commandHandler populates saga fields
  → stepState initialised: root nodes get tokenCount=1, all others get 0
  → saga placed in cache
  → scheduleReadyNodes()

scheduleReadyNodes()
  → collects nodes where tokenCount >= required AND status == PENDING
  → launches one coroutine per ready node under sagaLock

executeNode()  [inside sagaLock]
  → re-checks status == PENDING (guard against duplicate scheduling)
  → marks RUNNING atomically under mutex
  → persists to DB
  → calls executeStep()

executeStep()
  → runs node.execute()
  → if synchronous: calls completeNode()
  → if async: marks AWAITING_EVENT, schedules timeout coroutine, drains pending events

completeNode()
  → marks node SUCCESS
  → increments tokenCount on all successors
  → persists
  → schedules any newly-ready successors
  → if all nodes isDone: completeSaga()
```

### Async / event-driven steps

An async step calls its `execute` block (which fires an external action), then parks
itself as `AWAITING_EVENT`. The saga waits for an event to arrive via `dispatch()`.

```
External system fires event
  → dispatch() stores event in saga_event table
  → acquires sagaLock (withLockWaiting — waits, never skips)
  → drainPendingEvents()
    → reads PENDING events from DB
    → finds the AWAITING_EVENT step
    → calls handler inline (within the lock)
    → marks event PROCESSED
    → calls completeNode() → saga continues
```

### Branch execution

A branch block builds a chain of ConditionNode gates. Only gate-0 is wired to
predecessors — all others are reached via elseTarget.

- If a gate's condition is **true**: mark all remaining gates and their subtrees
  SKIPPED, then complete the gate normally so its branch steps receive tokens.
- If a gate's condition is **false**: mark the gate and its branch steps SKIPPED,
  increment tokenCount on elseTarget (the next gate), schedule it if ready.

The BranchJoinNode fires on the first token it receives — exactly one branch runs.

### Parallel execution

A ParallelNode fans out to all child branches simultaneously. Each child receives
a token and runs independently. A ParallelJoinNode at the end holds
requiredTokens = N (for Join.ALL) or 1 (for Join.ANY). It only fires
once enough tokens have arrived.

### Compensation

When any step throws or an onFailure handler fires:

1. The failed step is marked FAILED.
2. compensationStarted (AtomicBoolean) is set via compareAndSet(false, true) —
   ensures compensation only runs once even if two threads detect failure simultaneously.
3. All SUCCESS steps are collected in reverse order.
4. Each is marked COMPENSATING, its compensate block is called, then marked COMPENSATED.
5. completeSaga(success=false) fires.

---

## Locking

### Per-saga mutex (in-process)

Every LoadedSaga carries a kotlinx.coroutines.sync.Mutex. This guards all
reads and writes to stepState within a single JVM. All state transitions
(PENDING → RUNNING, token increments, status updates) happen inside
runtime.mutex.withLock { }.

### SagaLock (cross-node, pluggable)

SagaLock serialises execution of a saga across coroutines and across cluster nodes.
Two modes:

| Method | Behaviour | Used for |
|--------|-----------|----------|
| `withLock` | Try-once, returns null on contention | Recovery scans — skipping is acceptable |
| `withLockWaiting` | Suspends and retries until acquired | Event delivery — skipping is never acceptable |

**LocalSagaLock** uses a ConcurrentHashMap of coroutine Mutexes — safe within one JVM.

**RedisSagaLock** uses SET NX PX (set-if-absent with TTL) — safe across a cluster.
- `withLock`: single attempt, returns null immediately on failure.
- `withLockWaiting`: retries every 50ms until acquired.
- Lock release uses a Lua script for atomic check-and-delete, preventing a node
  from releasing a lock it no longer owns after TTL expiry.
- TTL (default 10s) acts as a dead-man switch: if a node crashes mid-execution,
  Redis expires the lock automatically and another node can claim the saga.

### Why two layers?

The mutex guards in-memory state within a single JVM (cheap, no network).
The SagaLock coordinates across nodes (Redis, or no-op locally).
A node always acquires both: mutex for state, SagaLock for cross-node exclusion.

### Why withLockWaiting for events?

dispatch() is called from outside the engine — potentially from a different coroutine
than the one currently executing the saga. If executeNode holds the SagaLock when
an event arrives, dispatch() must wait until execution finishes before delivering.
Using try-once (withLock) would silently drop the event if the lock is held.
withLockWaiting guarantees the event is always delivered once the current step finishes.

---

## Caching

The engine maintains an in-memory ConcurrentHashMap keyed by sagaId, storing the
LoadedSaga (instance + stepState + mutex + completion deferred) alongside a last-accessed
timestamp.

### What lives in cache

Only sagas this node is actively working on. A saga enters cache when:
- start() is called (new saga)
- getOrLoad() is called and the saga is not yet cached (lazy load from DB)
- restoreActiveSagas() runs on startup

### Cache eviction

- **Completed sagas**: evicted after a 5-minute TTL following completion,
  allowing any pending onComplete callbacks to fire.
- **AWAITING_EVENT sagas**: evicted after 1 minute of inactivity (parkAwaitingEvent).
  They consume memory but need no CPU. When their event arrives, dispatch() calls
  getOrLoad() which reloads them transparently from DB.

### Cache and the cluster

The cache is strictly local — each node only caches what it is working on.
There is no shared distributed cache. Consistency is maintained through the DB
(source of truth) and the SagaLock (prevents two nodes executing the same saga).

If a node restarts, restoreActiveSagas() reloads all active sagas from DB
back into the local cache. If another node picks up an orphaned saga via the
recovery scan, it loads it fresh from DB.

---

## Persistence

Every state change (status transition, token increment, timeout) is persisted
immediately via persist(runtime) inside the mutex, before the next action is taken.
The DB always reflects the current state — a node can crash at any point
and another node can resume from exactly where it left off.

### Tables

| Table | Purpose |
|-------|---------|
| `saga` | One row per saga instance. Stores type, status, payload (persisted fields as JSON), lastProcessedAt |
| `saga_step` | One row per step per saga. Stores status, tokenCount, timeoutAt |
| `saga_event` | Inbound events pending delivery. Status: PENDING → PROCESSED / FAILED |

### lastProcessedAt

Stamped on every persist() call. Used by the recovery scan to sort sagas by how
recently they were touched. Sagas not recently processed (orphans, crashed-node work)
float to the top of the scan. Actively processing sagas sink to the bottom.

### Write amplification

Each step transition involves multiple persists (RUNNING, AWAITING_EVENT / SUCCESS,
token increments on successors). This is intentional — it prioritises durability and
crash-safety over write efficiency. Each persist is a full upsert of the saga row
plus all step rows via JPA cascade.

---

## Event Store

Events are persisted to saga_event before delivery is attempted. This guarantees
at-least-once delivery even if the node crashes after storing but before processing.

### Delivery flow

```
dispatch(sagaId, event)
  → persist to saga_event (status=PENDING)
  → withLockWaiting(sagaId) — wait for any in-progress step to finish
  → drainPendingEvents()
    → SELECT all PENDING events for this sagaId
    → find the AWAITING_EVENT step
    → call handler inline (within the lock — no nested coroutine launch)
    → mark event PROCESSED
    → completeNode() → saga execution continues
```

Delivery is always inline within the lock. Launching a separate coroutine to deliver
would cause a re-entrant lock problem (the inner coroutine tries to acquire a lock
the outer coroutine already holds, and tryLock returns null, silently losing the event).

### Why no DB-level row locking?

The original design used SELECT FOR UPDATE SKIP LOCKED for cluster-safe delivery.
This was removed because the SagaLock already ensures only one node can execute
a given saga at a time. DB-level row locking is therefore redundant. The invariant
is: only one node ever calls drainPendingEvents for a given saga at a time.

---

## Recovery

### On startup

restoreActiveSagas() runs once on engine initialisation:
- Queries DB for all sagas with status RUNNING or COMPENSATING.
- Loads each into the local cache.
- If any step is COMPENSATING (node crashed mid-rollback): resumes compensation from
  the last persisted point.
- Otherwise: reschedules active timeout coroutines, calls scheduleReadyNodes().

### Periodic recovery (runner loop)

The SagaEngineRunner runs two targeted scans on a timer. These are recovery mechanisms
only — primary execution is entirely event-driven.

**recoverStuckSagas()** — every 10 seconds:
- Queries DB for RUNNING/COMPENSATING sagas sorted by lastProcessedAt ASC NULLS FIRST.
- Sagas with old or null lastProcessedAt are orphans from crashed nodes or genuinely stuck.
- For each candidate not in local cache: tries withLock (non-waiting).
  If another node holds the lock, that node is processing it — skip.
  If lock acquired and saga needs work: load into cache and schedule.

**recoverPendingEvents()** — every 5 seconds:
- Queries DB for RUNNING sagas that have PENDING rows in saga_event.
  Uses a subquery (not a JOIN with DISTINCT) to avoid SQL dialect issues with ORDER BY.
- These represent dropped work — a node stored an event but crashed before draining it.
- More frequent than stuck-saga scan because event delivery latency is user-visible.
- Calls drainPendingEvents() under withLock (non-waiting).

### Anti-starvation

Because recoverStuckSagas reads the N least-recently-processed sagas, there is no
fixed offset to rotate. As orphaned/stuck sagas get processed their lastProcessedAt
is updated and they naturally move to the bottom of future scans, making room for
the next batch of sagas that need attention.

---

## Cluster Behaviour

### Normal operation

In a healthy cluster each node owns the sagas it started or recovered. The SagaLock
prevents any two nodes from executing the same saga concurrently. A node that tries
to recover a saga already owned by another node will find the lock held and skip it.

### Node crash

When a node crashes:
- Its Redis locks expire after the TTL (default 10s).
- Its sagas stop receiving lastProcessedAt updates and float to the top of the recovery scan.
- Within one scan interval (10s) another node will detect the orphaned sagas,
  acquire their locks, load them from DB, and resume execution.
- Total failover latency is Redis TTL + scan interval = worst case ~20 seconds.

### Event delivery across nodes

An event can arrive at any node regardless of which node owns the saga.
The SagaLock ensures correct behaviour:

- If the owning node receives the event: withLockWaiting may need to wait briefly
  for an in-progress step to finish, then delivers inline.
- If a different node receives the event: it stores the event in saga_event, then
  tries withLockWaiting. The owning node holds the lock. The non-owning node waits.
  Eventually either the owning node's inline drainPendingEvents picks up the event,
  or the non-owning node acquires the lock after the owning node finishes and drains it.
  Either way the event is delivered exactly once.

### Cache size in a cluster

Each node's cache contains only the sagas it is currently executing — bounded
naturally by the SagaLock. A node that cannot acquire a lock for a saga simply
does not load it into cache. There is no fixed cache size limit to configure.
Memory pressure is governed by how many sagas a node is concurrently executing.

---

## Timeouts

When a step transitions to AWAITING_EVENT and has a timeout configured, a single
coroutine is launched with delay(remainingMs). When the delay expires:

- The coroutine acquires the SagaLock.
- Checks whether the step is still AWAITING_EVENT.
- If yes: failSaga() — compensation begins.
- If no (event already arrived and step completed): do nothing.

This requires no polling and fires exactly at the right time. On node restart,
restoreActiveSagas() re-creates these timeout coroutines for any steps that were
in AWAITING_EVENT with a non-null timeoutAt at the time of the crash.

---

## Kubernetes Considerations

| Concern | How it is handled |
|---------|------------------|
| Multiple replicas of same service | Redis SagaLock prevents duplicate saga execution across pods |
| Pod crash | Redis TTL expires the lock; recovery scan on surviving pods picks up orphans within ~20s |
| Pod startup | restoreActiveSagas() reloads this pod's sagas from DB; retries registry connection with backoff |
| Health checks | `/health` endpoint on each node wired to K8s liveness and readiness probes |
| Configuration | NODE_URL, REGISTRY_URL, NODE_PORT injected via K8s ConfigMap as environment variables |
| Node-to-node routing | K8s Service DNS — each service reachable at a stable DNS name regardless of pod IP |
| mTLS | Delegated to a service mesh (Istio / Linkerd) — transparent to the engine |
| Cache memory | Bounded naturally by the SagaLock — each node only caches what it actively executes |
| AWAITING_EVENT sagas | Evicted from cache after 1 minute idle; reloaded on event arrival via getOrLoad() |