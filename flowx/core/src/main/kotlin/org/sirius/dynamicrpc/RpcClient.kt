package org.sirius.dynamicrpc

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.runBlocking
import java.util.Base64

class RpcClient(private val baseUrl: String) {
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) { json() }
    }

    init {
        runBlocking { fetchRegistry() }
    }

    private var serviceName: String = ""
    private var methodName:  String = ""
    private var params: Map<String, Any?> = emptyMap()

    fun service(name: String) = apply { serviceName = name }
    fun method(name: String)  = apply { methodName  = name }
    fun param(name: String, value: Any?) = apply { params = params + (name to value) }

    suspend fun fetchRegistry() {
        val services: List<RemoteServiceInfo> = client.get("$baseUrl/registry").body()
        services.forEach { svc ->
            svc.methods.forEach { methodInfo ->
                // Reconstruct and register request descriptor
                val reqDescriptor = descriptorFromBase64(
                    methodInfo.requestDescriptorProto,
                    methodInfo.requestTypeName
                )
                Registry.registerType(methodInfo.requestTypeName, reqDescriptor)

                // Reconstruct and register response descriptor
                val respDescriptor = descriptorFromBase64(
                    methodInfo.responseDescriptorProto,
                    methodInfo.responseTypeName
                )
                Registry.registerType(methodInfo.responseTypeName, respDescriptor)
            }
        }
        Registry.registerRemoteServices(services)
    }

    private fun descriptorFromBase64(base64: String, typeName: String): Descriptors.Descriptor {
        val fileProto = DescriptorProtos.FileDescriptorProto.parseFrom(
            Base64.getDecoder().decode(base64)
        )
        val fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, arrayOf())
        return fileDescriptor.findMessageTypeByName(typeName)
            ?: error("Type '$typeName' not found in descriptor")
    }

    suspend fun invoke(): DynamicMessage {
        // Local shortcut
        val localService = Registry.getLocalService(serviceName)
        if (localService != null) {
            val method = localService.methods[methodName] ?: error("Method '$methodName' not found locally")
            val request = DynamicProtoGenerator.buildDynamicMessage(method.requestType, params)
            return method.handler(request)
        }

        // Remote invocation
        val remote = Registry.getRemoteService(serviceName)
            ?: error("Service '$serviceName' not found in registry")
        val methodInfo = remote.methods.find { it.name == methodName }
            ?: error("Method '$methodName' not found in remote service '$serviceName'")

        val reqDescriptor = Registry.getTypes()[methodInfo.requestTypeName]
            ?: error("No descriptor for request type '${methodInfo.requestTypeName}'")
        val respDescriptor = Registry.getTypes()[methodInfo.responseTypeName]
            ?: error("No descriptor for response type '${methodInfo.responseTypeName}'")

        val requestBytes = DynamicProtoGenerator.buildDynamicMessage(reqDescriptor, params).toByteArray()

        val responseBytes: ByteArray = client.post("${remote.url}/invoke/$serviceName/$methodName") {
            contentType(ContentType.Application.OctetStream)
            setBody(requestBytes)
        }.body()

        return DynamicMessage.parseFrom(respDescriptor, responseBytes)
    }
}