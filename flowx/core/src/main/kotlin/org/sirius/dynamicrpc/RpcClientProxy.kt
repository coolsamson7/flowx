package org.sirius.dynamicrpc

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import com.google.protobuf.DynamicMessage
import kotlinx.coroutines.runBlocking
import java.lang.reflect.Proxy
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.full.primaryConstructor

object RpcClientProxy {

    private val SCALAR_TYPES = setOf(
        Int::class, Long::class, String::class,
        Boolean::class, Double::class, Float::class
    )

    /**
     * Create a dynamic proxy for the given RPC service interface.
     * Each method call is translated into a fluent RpcClient invocation.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> create(client: RpcClient, serviceInterface: KClass<T>): T {
        val serviceName = serviceInterface.findAnnotation<RpcService>()
            ?.name?.ifEmpty { serviceInterface.simpleName!! }
            ?: serviceInterface.simpleName!!

        return Proxy.newProxyInstance(
            serviceInterface.java.classLoader,
            arrayOf(serviceInterface.java)
        ) { _, method, args ->

            // Resolve via Kotlin reflection to get parameter names
            val kFunction = serviceInterface.memberFunctions
                .find { it.name == method.name }
                ?: error("No KFunction found for method '${method.name}' on $serviceName")

            // Drop index 0 = 'this' receiver
            val kParams = kFunction.parameters.drop(1)

            // Build the fluent invocation
            var builder = client.service(serviceName).method(method.name)
            kParams.forEachIndexed { index, kParam ->
                builder = builder.param(kParam.name!!, args?.getOrNull(index))
            }

            val response: DynamicMessage = runBlocking { builder.invoke() }

            // Reconstruct the Kotlin return value from the DynamicMessage
            val returnClass = kFunction.returnType.classifier as? KClass<*>
                ?: error("Cannot determine return type for '${method.name}'")
            reconstructValue(response, returnClass)

        } as T
    }

    /**
     * Reconstruct a Kotlin value from a DynamicMessage:
     * - Scalars and String are stored in a single "value" field
     * - Data classes are reconstructed via their primary constructor
     */
    private fun reconstructValue(message: DynamicMessage, targetClass: KClass<*>): Any? {
        return if (targetClass in SCALAR_TYPES) {
            val field = message.descriptorForType.findFieldByName("value")
                ?: error("Expected 'value' field in scalar response for $targetClass")
            coerce(message.getField(field), targetClass)
        } else {
            val constructor = targetClass.primaryConstructor
                ?: error("No primary constructor for $targetClass")
            val ctorArgs = constructor.parameters.map { param ->
                val field = message.descriptorForType.findFieldByName(param.name!!)
                    ?: error("No field '${param.name}' in response message for $targetClass")
                coerce(message.getField(field), param.type.classifier as KClass<*>)
            }
            constructor.call(*ctorArgs.toTypedArray())
        }
    }

    private fun coerce(value: Any?, targetClass: KClass<*>): Any? {
        if (value == null) return null
        return when (targetClass) {
            Int::class     -> (value as Number).toInt()
            Long::class    -> (value as Number).toLong()
            Float::class   -> (value as Number).toFloat()
            Double::class  -> (value as Number).toDouble()
            Boolean::class -> value as Boolean
            String::class  -> value.toString()
            else           -> value
        }
    }
}

/** Convenience reified extension */
inline fun <reified T : Any> RpcClientProxy.create(client: RpcClient): T =
    create(client, T::class)