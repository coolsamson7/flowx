package org.sirius.flowx

import jakarta.annotation.PostConstruct
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component

@Component
class SagaRegistry(private val context: ApplicationContext) {

    // name -> saga class  (used by storage to reconstruct instances)
    private val registry        = mutableMapOf<String, Class<out Saga<*>>>()

    // command class -> saga class  (used by engine.send())
    private val commandRegistry = mutableMapOf<Class<out Command>, Class<out Saga<*>>>()

    fun scanAndRegister() {
        val beans = context.getBeansWithAnnotation(SagaType::class.java)
        beans.forEach { (_, bean) ->
            val clazz      = bean::class.java
            val sagaType   = clazz.getAnnotation(SagaType::class.java)
            val startedBy  = clazz.getAnnotation(StartedBy::class.java)

            @Suppress("UNCHECKED_CAST")
            val sagaClass  = clazz as Class<out Saga<*>>

            registry[sagaType.name] = sagaClass

            if (startedBy != null) {
                commandRegistry[startedBy.command.java] = sagaClass
            }
        }
    }

    fun get(name: String): Class<out Saga<*>> =
        registry[name] ?: throw IllegalStateException("Saga class not found for name: $name")

    fun getByCommand(commandClass: Class<out Command>): Class<out Saga<*>>? =
        commandRegistry[commandClass]

    fun allRegistered(): Map<String, Class<out Saga<*>>> = registry.toMap()

    @PostConstruct
    fun init() {
        scanAndRegister()
        println("Registered sagas: ${allRegistered().keys}")
    }
}