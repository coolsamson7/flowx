package org.sirius.flowx

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

enum class EventOutcome { COMPLETE, FAIL }

enum class Join { ALL, ANY }

// -------------------- NODES --------------------

open class Node<T : Any>(
    val id: String,
    val execute: suspend T.() -> Unit = {},
    val compensate: suspend T.() -> Unit = {},
    val condition: (T.() -> Boolean)? = null,
    val retries: Int = 0,
    val timeoutMillis: Long? = null,
    val handlers: Map<Class<out Event>, (T, Event) -> EventOutcome> = emptyMap()
) {
    val successors:   MutableList<Node<T>> = mutableListOf()
    val predecessors: MutableList<Node<T>> = mutableListOf()

    val isAsync: Boolean get() = handlers.isNotEmpty()
}

// FIX: init{} removed.
// The old init{} added `this` as predecessor to every child during construction.
// The DSL then checked `if (child.predecessors.isEmpty())` to wire
// parallelNode → child — that check was always false, so parallelNode.successors
// was always empty and children never received tokens.
// Wiring is now done entirely in SagaDSL.parallel().
class ParallelNode<T : Any>(
    id: String,
    val children: List<Node<T>>,
    val join: Join = Join.ALL
) : Node<T>(id)

// FIX: new explicit join node for parallel blocks.
// Previously the step *after* the parallel block was wired directly to all
// leaf nodes and fired as soon as tokenCount >= 1 — effectively always ANY.
// ParallelJoinNode carries requiredTokens so the engine holds it until
// ALL (or ANY) branches have delivered their token.
class ParallelJoinNode<T : Any>(
    id: String,
    val requiredTokens: Int   // ALL → number of leaf branches; ANY → 1
) : Node<T>(id)

// FIX: elseTarget added.
// Previously ALL ConditionNodes were wired as direct successors of the
// preceding step, so all got tokenCount=1 at start and all branches ran.
// Now the gates are chained: gate[0] is the only one wired to lastNodes.
// When gate[0]'s predicate is false the engine sends a token to elseTarget
// (gate[1], or the BranchJoinNode if this is the last branch).
// branchCondition is evaluated in SagaEngine.executeStep().
class ConditionNode<T : Any>(
    id: String,
    val branchCondition: (T) -> Boolean,
    val joinNodeId: String
) : Node<T>(id) {
    // Set by SagaDSL.branch() after all gates are built.
    // Points to the next gate, or to BranchJoinNode when no branch matches.
    var elseTarget: Node<T>? = null
}

// Convergence node for branch blocks.
// Fires on the first token it receives — exactly one branch runs.
class BranchJoinNode<T : Any>(id: String) : Node<T>(id)

// -------------------- STEP STATE --------------------

data class StepState(
    var status: StepStatus = StepStatus.PENDING,
    var tokenCount: Int = 0,
    var timeoutAt: Long? = null
)

enum class StepStatus {
    PENDING,
    RUNNING,
    AWAITING_EVENT,
    SUCCESS,
    SKIPPED,
    COMPENSATING,
    COMPENSATED,
    FAILED
}

data class SagaContext(
    val crashAfterStep: String? = null,
    val failOn: String? = null
)

class SagaDefinition<T : Any>(
    val nodes: List<Node<T>>,
    val commandHandler: ((T, Command) -> Unit)? = null
)