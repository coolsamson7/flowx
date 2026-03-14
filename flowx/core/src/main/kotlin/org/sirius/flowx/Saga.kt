package org.sirius.flowx
/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import java.lang.reflect.Field
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

interface Command

// Marks a saga class as being started by a specific command type
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class StartedBy(val command: KClass<out Command>)

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class Persisted

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
annotation class SagaType(val name: String)

class SagaDescriptor(
    val type: Class<*>,
    val persistedFields: List<Field>
)

object SagaDescriptorCache {
    private val cache = ConcurrentHashMap<Class<*>, SagaDescriptor>()

    fun get(clazz: Class<*>): SagaDescriptor {
        return cache.computeIfAbsent(clazz) {

            val fields = mutableListOf<Field>()
            var current: Class<*>? = clazz
            while (current != null && current != Any::class.java) {
                current.declaredFields
                    .filter { it.isAnnotationPresent(Persisted::class.java) }
                    .forEach { field ->
                        field.isAccessible = true
                        fields.add(field)
                    }
                current = current.superclass
            }

            SagaDescriptor(clazz, fields)
        }
    }
}

interface Saga<T : Any> {
    var id: String
    fun definition() : SagaDefinition<T>
}

abstract class AbstractSaga<T: Any>() : Saga<T> {
    @Persisted override var id: String = ""
}
