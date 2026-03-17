package org.sirius.dynamicrpc

import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.serialization.kotlinx.json.*

fun main() {
    // serviceName → RemoteServiceInfo (for routing/invocation)
    val services = mutableMapOf<String, RemoteServiceInfo>()
    // nodeUrl → NodeInfo (for introspection/UI)
    val nodes = mutableMapOf<String, NodeInfo>()

    embeddedServer(Netty, port = 9000) {
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
        }
    }.start(wait = true)
}