package org.sirius.dynamicrpc

import com.google.protobuf.DynamicMessage
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

class IngressNode(
    private val springAppClass: KClass<*>,
    registryUrl: String = System.getenv("REGISTRY_URL") ?: "http://localhost:9000",
    private val nodeUrl:  String = System.getenv("NODE_URL")  ?: "http://localhost:8080",
    private val nodePort: Int    = System.getenv("NODE_PORT")?.toInt() ?: 8080
) : RpcNode(registryUrl) {

    override fun nodeName() = "ServiceNode[$nodeUrl]"

    override fun start() {
        // serviceName → RemoteServiceInfo (for routing/invocation)
        val services = mutableMapOf<String, RemoteServiceInfo>()
        // nodeUrl → NodeInfo (for introspection/UI)
        val nodes = mutableMapOf<String, NodeInfo>()

        // 1. Start Spring — scans @Component beans
        val ctx = SpringApplication.run(springAppClass.java, *emptyArray<String>())

        // 2. Scan @RpcService beans → populate local Registry
        Registry.scanAndRegister(ctx, url = nodeUrl)
        printLocalState()

        // 4. Start Ktor — serve incoming RPC calls
        println("[${nodeName()}] Listening on port $nodePort...")
        embeddedServer(Netty, port = nodePort) {
            install(ContentNegotiation) { json() }
            routing {
                // Node pushes its services + full metadata on startup
                post("/register") {
                    val registration = call.receive<NodeRegistration>()

                    // Store per-service routing info
                    registration.services.forEach { info ->
                        services[info.name] = info
                        println("[Registry] Service '${info.name}' @ ${registration.nodeInfo.url} " +
                                "(${info.methods.size} methods)")
                    }

                    // Store full node metadata
                    nodes[registration.nodeInfo.url] = registration.nodeInfo
                    println("[Registry] Node '${registration.nodeInfo.url}' registered " +
                            "(${registration.nodeInfo.services.size} services, " +
                            "${registration.nodeInfo.types.size} types)")

                    call.respond(mapOf("status" to "ok"))
                }

                // Full service list for routing (used by RpcClient/nodes on startup)
                get("/services") {
                    call.respond(services.values.toList())
                }

                // Full node list for introspection/UI
                get("/nodes") {
                    call.respond(nodes.values.toList())
                }

                // Single node detail
                get("/nodes/{url}") {
                    val url = call.parameters["url"]!!
                    val node = nodes[url] ?: return@get call.respond(
                        io.ktor.http.HttpStatusCode.NotFound,
                        mapOf("error" to "Node '$url' not found")
                    )
                    call.respond(node)
                }

                // Health check
                get("/health") {
                    call.respond(mapOf(
                        "status"   to "up",
                        "nodes"    to nodes.size,
                        "services" to services.size
                    ))
                }

                post("/invoke/{service}/{method}") {
                    val serviceName  = call.parameters["service"]!!
                    val methodName   = call.parameters["method"]!!
                    val payloadBytes = call.receive<ByteArray>()

                    val method = Registry.getLocalService(serviceName)?.methods?.get(methodName)
                        ?: return@post call.respondText(
                            "Service '$serviceName.$methodName' not found",
                            status = HttpStatusCode.NotFound
                        )

                    val request  = DynamicMessage.parseFrom(method.requestType, payloadBytes)
                    val response = method.handler(request)
                    call.respondBytes(response.toByteArray())
                }
            }
        }.start(wait = true)
    }
}