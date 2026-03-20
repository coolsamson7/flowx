package org.sirius.dynamicrpc

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.protobuf.DynamicMessage
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.springframework.boot.SpringApplication
import kotlin.reflect.KClass
import io.ktor.client.call.*
import io.ktor.client.plugins.contentnegotiation.*

class IngressNode(
    private val springAppClass: KClass<*>,
    registryUrl: String = System.getenv("REGISTRY_URL") ?: "http://localhost:9000",
    private val nodeUrl: String = System.getenv("NODE_URL") ?: "http://localhost:9000",
    private val nodePort: Int = System.getenv("NODE_PORT")?.toInt() ?: 9000
) : RpcNode(registryUrl) {

    private val mapper = jacksonObjectMapper()

    override fun nodeName() = "IngressNode[$nodeUrl]"

    override fun start() {
        val services = mutableMapOf<String, RemoteServiceInfo>()
        val nodes = mutableMapOf<String, NodeInfo>()

        // Start Spring — only needed if the ingress itself hosts any beans
        val ctx = SpringApplication.run(springAppClass.java, *emptyArray<String>())

        // Scan local @RpcService beans (likely none on a pure ingress)
        Registry.scanAndRegister(ctx, url = nodeUrl)
        printLocalState()

        println("[${nodeName()}] Listening on port $nodePort...")
        embeddedServer(Netty, port = nodePort) {
            install(ContentNegotiation) { json() }
            routing {

                // ── Registry endpoints ────────────────────────────────────────

                post("/register") {
                    val registration = call.receive<NodeRegistration>()

                    registration.services.forEach { info ->
                        services[info.name] = info
                        info.methods.forEach { Registry.hydrateMethods(it) }
                        println("[${nodeName()}] Service '${info.name}' @ ${registration.nodeInfo.url} " +
                                "(${info.methods.size} methods)")
                    }

                    nodes[registration.nodeInfo.url] = registration.nodeInfo
                    println("[${nodeName()}] Node '${registration.nodeInfo.url}' registered " +
                            "(${registration.nodeInfo.services.size} services, " +
                            "${registration.nodeInfo.types.size} types)")

                    Registry.registerRemoteServices(registration.services)
                    call.respond(mapOf("status" to "ok"))
                }

                get("/services") {
                    call.respond(services.values.toList())
                }

                get("/nodes") {
                    call.respond(nodes.values.toList())
                }

                get("/nodes/{url}") {
                    val url = call.parameters["url"]!!
                    val node = nodes[url] ?: return@get call.respond(
                        HttpStatusCode.NotFound,
                        mapOf("error" to "Node '$url' not found")
                    )
                    call.respond(node)
                }

                get("/health") {
                    call.respond(
                        mapOf(
                            "status" to "up",
                            "nodes" to nodes.size,
                            "services" to services.size
                        )
                    )
                }

                // ── Invoke — JSON in, JSON out, Protobuf internally ───────────

                post("/invoke/{service}/{method}") {
                    val serviceName = call.parameters["service"]!!
                    val methodName = call.parameters["method"]!!

                    // Look up remote ServiceNode for this service
                    val remote = Registry.getRemoteService(serviceName)
                        ?: return@post call.respondText(
                            "Service '$serviceName' not found",
                            status = HttpStatusCode.NotFound
                        )

                    val methodInfo = remote.methods.find { it.name == methodName }
                        ?: return@post call.respondText(
                            "Method '$methodName' not found in service '$serviceName'",
                            status = HttpStatusCode.NotFound
                        )

                    // Resolve descriptors (hydrated at registration time)
                    val reqDescriptor = Registry.getTypes()[methodInfo.requestTypeName]
                        ?: return@post call.respondText(
                            "No descriptor for '${methodInfo.requestTypeName}'",
                            status = HttpStatusCode.InternalServerError
                        )
                    val respDescriptor = Registry.getTypes()[methodInfo.responseTypeName]
                        ?: return@post call.respondText(
                            "No descriptor for '${methodInfo.responseTypeName}'",
                            status = HttpStatusCode.InternalServerError
                        )

                    // JSON → Map → DynamicMessage → bytes
                    val params: Map<String, Any?> = mapper.readValue(
                        call.receiveText(),
                        mapper.typeFactory.constructMapType(
                            Map::class.java, String::class.java, Any::class.java
                        )
                    )

                    val requestBytes = DynamicProtoGenerator.buildDynamicMessage(reqDescriptor, params)
                        .toByteArray()

                    // Forward protobuf bytes to the owning ServiceNode
                    val responseBytes: ByteArray = httpClient.post("${remote.url}/invoke/$serviceName/$methodName") {
                        contentType(ContentType.Application.OctetStream)
                        setBody(requestBytes)
                    }.body<ByteArray>()

                    // bytes → DynamicMessage → Map → JSON
                    val responseMessage = DynamicMessage.parseFrom(respDescriptor, responseBytes)
                    call.respondText(
                        mapper.writeValueAsString(toMap(responseMessage)),
                        ContentType.Application.Json
                    )
                }
            }
        }.start(wait = true)
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Recursively convert a DynamicMessage to a plain Map so Jackson can
     * serialize it to JSON. Nested @RpcType messages are converted the same way.
     */
    private fun toMap(message: DynamicMessage): Map<String, Any?> =
        message.allFields.entries.associate { (field, value) ->
            field.name to when (value) {
                is DynamicMessage -> toMap(value)
                else -> value
            }
        }
}