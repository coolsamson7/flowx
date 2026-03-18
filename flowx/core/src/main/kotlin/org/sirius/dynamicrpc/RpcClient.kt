package org.sirius.dynamicrpc

import com.google.protobuf.DynamicMessage
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.runBlocking
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties

class RpcClient(private val registryUrl: String = "http://localhost:9000") {

    private val client = HttpClient(CIO) {
        install(ContentNegotiation) { json() }
    }

    private var serviceName: String = ""
    private var methodName:  String = ""
    private var params: Map<String, Any?> = emptyMap()

    init {
        runBlocking { fetchRegistry() }
    }

    fun service(name: String) = apply {
        serviceName = name
        params = emptyMap()  // reset params on each new service call
    }

    fun method(name: String) = apply { methodName = name }

    // Primitive or already-known value
    fun param(name: String, value: Any?) = apply {
        params = params + (name to value)
    }

    // Single @RpcType — flatten all properties to top-level fields
    fun param(value: Any) = apply {
        val kClass = value::class
        if (kClass.findAnnotation<RpcType>() != null) {
            kClass.memberProperties.forEach { prop ->
                params = params + (prop.name to prop.getter.call(value))
            }
        } else {
            error("param(value) only supports @RpcType classes, got ${kClass.simpleName}")
        }
    }

    // Named compound param — kept as object, recursed in buildDynamicMessage
    fun param(name: String, value: Any, nested: Boolean) = apply {
        params = params + (name to value)
    }

    suspend fun fetchRegistry() {
        val services: List<RemoteServiceInfo> = client.get("$registryUrl/services").body()
        services.forEach { svc ->
            svc.methods.forEach { Registry.hydrateMethods(it) }
        }
        Registry.registerRemoteServices(services)
    }

    suspend fun invoke(): DynamicMessage {
        // Local shortcut
        val localService = Registry.getLocalService(serviceName)
        if (localService != null) {
            val method = localService.methods[methodName]
                ?: error("Method '$methodName' not found locally in '$serviceName'")
            val request = DynamicProtoGenerator.buildDynamicMessage(method.requestType, params)
            return method.handler(request)
        }

        // Remote invocation
        val remote = Registry.getRemoteService(serviceName)
            ?: error("Service '$serviceName' not found in registry")
        val methodInfo = remote.methods.find { it.name == methodName }
            ?: error("Method '$methodName' not found in remote service '$serviceName'")

        val reqDescriptor = Registry.getTypes()[methodInfo.requestTypeName]
            ?: error("No descriptor for '${methodInfo.requestTypeName}'")
        val respDescriptor = Registry.getTypes()[methodInfo.responseTypeName]
            ?: error("No descriptor for '${methodInfo.responseTypeName}'")

        val requestBytes = DynamicProtoGenerator
            .buildDynamicMessage(reqDescriptor, params)
            .toByteArray()

        val responseBytes: ByteArray = client
            .post("${remote.url}/invoke/$serviceName/$methodName") {
                contentType(ContentType.Application.OctetStream)
                setBody(requestBytes)
            }.body()

        return DynamicMessage.parseFrom(respDescriptor, responseBytes)
    }
}