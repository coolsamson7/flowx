package org.sirius.dynamicrpc

import com.google.protobuf.DynamicMessage
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation as ServerContentNegotiation
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.runBlocking
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class Application

fun main() {
    val registryUrl = System.getenv("REGISTRY_URL") ?: "http://localhost:9000"
    val nodeUrl     = System.getenv("NODE_URL")     ?: "http://localhost:8080"
    val nodePort    = System.getenv("NODE_PORT")?.toInt() ?: 8080

    // 1. Start Spring — scans @Component beans
    val ctx = runApplication<Application>()

    // 2. Scan local @RpcService beans → populate local Registry
    Registry.scanAndRegister(ctx, url = nodeUrl)

    // 3. Print what we registered locally
    println("\n=== Node $nodeUrl ===")
    println("Local services:")
    Registry.getAllLocalServices().forEach { (svcName, svc) ->
        println("  - $svcName")
        svc.methods.forEach { (methodName, method) ->
            val req  = method.requestType.fields.joinToString(", ") { "${it.name}:${it.type.name}" }
            val resp = method.responseType.fields.joinToString(", ") { "${it.name}:${it.type.name}" }
            println("      $methodName  req($req)  resp($resp)")
        }
    }
    println("Local types:")
    Registry.getTypes().forEach { (name, descriptor) ->
        val fields = descriptor.fields.joinToString(", ") { "${it.name}:${it.type.name}" }
        println("  - $name($fields)")
    }

    val httpClient = HttpClient(CIO) {
        install(ClientContentNegotiation) { json() }
    }

    runBlocking {
        // 4. Push our services + full metadata to the central registry
        val registration = NodeRegistration(
            nodeInfo = Registry.buildNodeInfo(nodeUrl),
            services = Registry.getAllRemoteServices().values.toList()
        )

        if (registration.services.isNotEmpty()) {
            try {
                httpClient.post("$registryUrl/register") {
                    contentType(ContentType.Application.Json)
                    setBody(registration)
                }
                println("\nRegistered with central registry @ $registryUrl")
            } catch (e: Exception) {
                println("\nWARN: Could not reach central registry @ $registryUrl — running standalone")
            }
        }

        // 5. Pull all services from central registry and hydrate remote descriptors
        try {
            val allServices: List<RemoteServiceInfo> = httpClient
                .get("$registryUrl/services")
                .body()

            allServices.forEach { svc ->
                if (Registry.getLocalService(svc.name) == null) {
                    svc.methods.forEach { Registry.hydrateMethods(it) }
                    Registry.registerRemoteServices(listOf(svc))
                    println("Discovered remote service '${svc.name}' @ ${svc.url}")
                }
            }
        } catch (e: Exception) {
            println("WARN: Could not fetch services from central registry — no remote services available")
        }
    }

    // 6. Start Ktor — handles incoming RPC calls from other nodes
    println("\nNode listening on port $nodePort...")
    embeddedServer(Netty, port = nodePort) {
        install(ServerContentNegotiation) { json() }
        routing {

            // Liveness probe
            get("/health") {
                call.respond(mapOf("status" to "up", "url" to nodeUrl))
            }

            // RPC invocation — called directly by other nodes/clients
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