package org.sirius.flowx

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

// -------------------- STEP BUILDER --------------------

class StepBuilder<T : Any>(val id: String) {
    private var executeBlock: suspend T.() -> Unit = {}
    private var compensateBlock: suspend T.() -> Unit = {}
    private var conditionBlock: (T.() -> Boolean)? = null
    val handlers = mutableMapOf<Class<out Event>, (T, Event) -> EventOutcome>()
    private var retriesCount: Int = 0
    private var timeoutMillisValue: Long? = null

    fun execute(block: suspend T.() -> Unit)    { executeBlock    = block }
    fun compensate(block: suspend T.() -> Unit) { compensateBlock = block }
    fun retries(n: Int)    { retriesCount       = n  }
    fun timeout(ms: Long)  { timeoutMillisValue = ms }

    fun condition(block: T.() -> Boolean) { conditionBlock = block }

    inline fun <reified E : Event> onSuccess(noinline block: T.(E) -> Unit = { }) {
        handlers[E::class.java] = { t, e -> t.block(e as E); EventOutcome.COMPLETE }
    }

    inline fun <reified E : Event> onFailure(noinline block: T.(E) -> Unit = { }) {
        handlers[E::class.java] = { t, e -> t.block(e as E); EventOutcome.FAIL }
    }

    fun build(): Node<T> = Node(
        id            = id,
        execute       = executeBlock,
        compensate    = compensateBlock,
        condition     = conditionBlock,
        retries       = retriesCount,
        timeoutMillis = timeoutMillisValue,
        handlers      = handlers.toMap()
    )
}

// -------------------- BRANCH BUILDER --------------------

class BranchBuilder<T : Any> {
    internal val branches      = mutableListOf<Pair<T.() -> Boolean, SagaDSL<T>.() -> Unit>>()
    internal var otherwiseBlock: (SagaDSL<T>.() -> Unit)? = null

    fun on(predicate: T.() -> Boolean, block: SagaDSL<T>.() -> Unit) {
        branches += predicate to block
    }

    fun otherwise(block: SagaDSL<T>.() -> Unit) {
        otherwiseBlock = block
    }
}

// -------------------- SAGA DSL --------------------

class SagaDSL<T : Any> {

    internal val nodes: MutableList<Node<T>>     = mutableListOf()
    internal var lastNodes: MutableList<Node<T>> = mutableListOf()
    var commandHandler: ((T, Command) -> Unit)? = null

    inline fun <reified C : Command> on(noinline block: T.(C) -> Unit) {
        commandHandler = { saga, cmd -> saga.block(cmd as C) }
    }

    // ── step ─────────────────────────────────────────────────────────────────

    fun step(id: String, block: StepBuilder<T>.() -> Unit): SagaDSL<T> {
        wire(StepBuilder<T>(id).apply(block).build())
        return this
    }

    // ── parallel ─────────────────────────────────────────────────────────────
    //
    // Graph shape:
    //
    //   [prev] → ParallelNode → child1 ──┐
    //                         → child2 ──┤→ ParallelJoinNode → [next]
    //                         → child3 ──┘
    //
    // FIX 1: ParallelNode.init{} no longer pre-populates child.predecessors,
    // so `child.predecessors.isEmpty()` now correctly identifies root children.
    //
    // FIX 2: An explicit ParallelJoinNode replaces the old scheme of wiring
    // [next] directly to every leaf.  The join node's requiredTokens is
    // leaves.size (JOIN.ALL) or 1 (JOIN.ANY), and the engine only fires it
    // once that threshold is reached.

    fun parallel(join: Join = Join.ALL, block: SagaDSL<T>.() -> Unit): SagaDSL<T> {
        val subDSL       = SagaDSL<T>().apply(block)
        val parallelNode = ParallelNode<T>("parallel-${nodes.size}", subDSL.nodes.toList(), join)

        // [prev] → parallelNode
        lastNodes.forEach {
            it.successors.add(parallelNode)
            parallelNode.predecessors.add(it)
        }

        // parallelNode → root children (no predecessors yet — init{} is gone)
        subDSL.nodes.forEach { child ->
            if (child.predecessors.isEmpty()) {
                parallelNode.successors.add(child)
                child.predecessors.add(parallelNode)
            }
        }

        // leaves of the parallel block
        val leaves = subDSL.nodes.filter { it.successors.isEmpty() }

        val requiredTokens = when (join) {
            Join.ALL -> leaves.size
            Join.ANY -> 1
        }
        val joinNode = ParallelJoinNode<T>("parallel-join-${nodes.size}", requiredTokens)

        leaves.forEach { leaf ->
            leaf.successors.add(joinNode)
            joinNode.predecessors.add(leaf)
        }

        nodes.add(parallelNode)
        nodes.addAll(subDSL.nodes)
        nodes.add(joinNode)
        lastNodes = mutableListOf(joinNode)
        return this
    }

    // ── branch ───────────────────────────────────────────────────────────────
    //
    // Graph shape (two branches):
    //
    //   [prev] → Gate0 ──(true)──→ stepA → BranchJoinNode → [next]
    //              │(false)
    //              └──→ Gate1 ──(true)──→ stepB → BranchJoinNode
    //                     │(false)
    //                     └──→ BranchJoinNode   (no branch matched)
    //
    // FIX: Gates are chained, not fanned out.
    // Only Gate0 is wired to [prev], so only Gate0 gets a token.
    // On false, Gate0 passes its token to Gate1 (elseTarget).
    // On true, Gate0 completes normally and its successors (stepA …) get tokens.
    // The engine evaluates branchCondition in executeStep when it sees a ConditionNode.

    fun branch(block: BranchBuilder<T>.() -> Unit): SagaDSL<T> {
        val builder = BranchBuilder<T>().apply(block)

        val joinNodeId = "branch-join-${nodes.size}"
        val joinNode   = BranchJoinNode<T>(joinNodeId)

        val alwaysTrue: T.() -> Boolean = { true }
        val allBranches: List<Pair<T.() -> Boolean, SagaDSL<T>.() -> Unit>> = buildList {
            addAll(builder.branches)
            builder.otherwiseBlock?.let { add(alwaysTrue to it) }
        }

        // Build all gate nodes first so we can chain elseTargets afterwards
        val gateNodes = allBranches.mapIndexed { idx, (predicate, subBlock) ->
            val subDSL   = SagaDSL<T>().apply(subBlock)
            val gateNode = ConditionNode<T>(
                id              = "branch-gate-${nodes.size}-$idx",
                branchCondition = { t -> t.predicate() },
                joinNodeId      = joinNodeId
            )

            // gate → first branch step(s)
            subDSL.nodes.filter { it.predecessors.isEmpty() }.forEach { first ->
                gateNode.successors.add(first)
                first.predecessors.add(gateNode)
            }

            // last branch step(s) → joinNode
            subDSL.nodes.filter { it.successors.isEmpty() }.forEach { last ->
                last.successors.add(joinNode)
                joinNode.predecessors.add(last)
            }

            nodes.add(gateNode)
            nodes.addAll(subDSL.nodes)

            gateNode
        }

        // Chain gates: gate[i].elseTarget = gate[i+1], last gate → joinNode
        gateNodes.forEachIndexed { i, gate ->
            gate.elseTarget = if (i + 1 < gateNodes.size) gateNodes[i + 1] else joinNode
        }

        // Only the first gate is wired to [prev] — the rest are reached via elseTarget
        val firstGate = gateNodes.first()
        lastNodes.forEach {
            it.successors.add(firstGate)
            firstGate.predecessors.add(it)
        }

        nodes.add(joinNode)
        lastNodes = mutableListOf(joinNode)
        return this
    }

    // ── internal ─────────────────────────────────────────────────────────────

    private fun wire(node: Node<T>) {
        nodes.add(node)
        lastNodes.forEach {
            it.successors.add(node)
            node.predecessors.add(it)
        }
        lastNodes = mutableListOf(node)
    }

    fun build(): SagaDefinition<T> = SagaDefinition(nodes, commandHandler)
}

// -------------------- TOP-LEVEL DSL ENTRY POINT --------------------

fun <T : Any> saga(block: SagaDSL<T>.() -> Unit): SagaDefinition<T> =
    SagaDSL<T>().apply(block).build()