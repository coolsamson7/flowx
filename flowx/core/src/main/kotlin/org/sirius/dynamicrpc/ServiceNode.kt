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
import io.ktor.serialization.jackson.*

class ServiceNode(
    private val springAppClass: KClass<*>,
    registryUrl: String = System.getenv("REGISTRY_URL") ?: "http://localhost:9000",
    private val nodeUrl:  String = System.getenv("NODE_URL")  ?: "http://localhost:8080",
    private val nodePort: Int    = System.getenv("NODE_PORT")?.toInt() ?: 8080
) : RpcNode(registryUrl) {

    override fun nodeName() = "ServiceNode[$nodeUrl]"

    override fun start() {
        // 1. Start Spring — scans @Component beans
        val ctx = SpringApplication.run(springAppClass.java, *emptyArray<String>())

        // 2. Scan @RpcService beans → populate local Registry
        Registry.scanAndRegister(ctx, url = nodeUrl)
        printLocalState()

        // 3. Push to registry, then pull remote services
        val registration = NodeRegistration(
            nodeInfo = Registry.buildNodeInfo(nodeUrl),
            services = Registry.getAllRemoteServices().values.toList()
        )
        if (registration.services.isNotEmpty()) pushToRegistry(registration)
        connect()

        // 4. Start Ktor — serve incoming RPC calls
        println("[${nodeName()}] Listening on port $nodePort...")
        embeddedServer(Netty, port = nodePort) {
            install(ContentNegotiation) {
                //json()
                jackson()
            }
            routing {
                get("/health") {
                    call.respond(mapOf("status" to "up", "node" to nodeUrl))
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