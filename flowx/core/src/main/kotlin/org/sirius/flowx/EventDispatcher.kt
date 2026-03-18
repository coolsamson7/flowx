package org.sirius.flowx

import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

typealias EventExtractor<T> = (T) -> Pair<String, Event>

interface EventSink {
    fun dispatch(sagaId: String, event: Event)
}

object EventDispatcher {

    private val extractors = ConcurrentHashMap<Class<*>, (Any) -> Pair<String, Event>>()
    private val cache      = ConcurrentHashMap<Class<*>, Optional<(Any) -> Pair<String, Event>>>()

    private var sink: EventSink? = null

    // ── Wiring ───────────────────────────────────────────────────────────────

    fun bind(sink: EventSink) {
        this.sink = sink
    }

    // ── Registration ─────────────────────────────────────────────────────────

    fun <T : Any> register(clazz: Class<T>, extractor: EventExtractor<T>) {
        @Suppress("UNCHECKED_CAST")
        extractors[clazz] = extractor as (Any) -> Pair<String, Event>
        cache.clear()
    }

    fun <T : Any> register(clazz: kotlin.reflect.KClass<T>, extractor: EventExtractor<T>) =
        register(clazz.java, extractor)

    // ── Dispatch ─────────────────────────────────────────────────────────────

    /**
     * Accepts a plain [Event] or any envelope with a registered extractor.
     * Resolves (sagaId, Event) and forwards to the bound [EventSink].
     */
    fun dispatch(obj: Any) {
        val (sagaId, event) = when (obj) {
            is Event -> obj.sagaId to obj
            else     -> resolve(obj::class.java)?.invoke(obj)
                ?: error("No EventExtractor registered for ${obj::class.simpleName}")
        }
        checkNotNull(sink) { "EventDispatcher has no bound sink — call bind() first" }
            .dispatch(sagaId, event)
    }

    // ── Hierarchy walk ───────────────────────────────────────────────────────

    private fun resolve(clazz: Class<*>): ((Any) -> Pair<String, Event>)? {
        cache[clazz]?.let { return it.orElse(null) }
        val found = findInHierarchy(clazz)
        cache[clazz] = Optional.ofNullable(found)
        return found
    }

    private fun findInHierarchy(clazz: Class<*>?): ((Any) -> Pair<String, Event>)? {
        if (clazz == null || clazz == Any::class.java) return null
        extractors[clazz]?.let { return it }
        clazz.interfaces.forEach { iface ->
            findInHierarchy(iface)?.let { return it }
        }
        return findInHierarchy(clazz.superclass)
    }
}