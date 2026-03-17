package org.sirius.dynamicrpc

import com.google.protobuf.DynamicMessage
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.serialization.kotlinx.json.*
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class Application

fun main() {
    val serverUrl = "http://localhost:8080"
    val ctx = runApplication<Application>()

    Registry.scanAndRegister(ctx, url = serverUrl)

    println("\nRegistered types:")
    Registry.getTypes().forEach { (name, descriptor) ->
        println(" - $name")
        descriptor.fields.forEach { field ->
            println("     ${field.name}: ${field.type.name}")
        }
    }

    println("\nRegistered services:")
    Registry.getAllLocalServices().forEach { (serviceName, service) ->
        println(" - $serviceName")
        service.methods.forEach { (methodName, method) ->
            val reqFields = method.requestType.fields.joinToString(", ") { "${it.name}: ${it.type.name}" }
            val respFields = method.responseType.fields.joinToString(", ") { "${it.name}: ${it.type.name}" }
            println("     $methodName")
            println("       req:  ${method.requestType.name}($reqFields)")
            println("       resp: ${method.responseType.name}($respFields)")
        }
    }

    embeddedServer(Netty, port = 8080) {
        install(ContentNegotiation) { json() }

        routing {
            // Registry: returns full service/method/descriptor metadata
            get("/registry") {
                call.respond(Registry.getAllRemoteServices().values.toList())
            }

            // Invoke: service+method in URL, raw proto bytes as body
            post("/invoke/{service}/{method}") {
                val serviceName = call.parameters["service"]!!
                val methodName  = call.parameters["method"]!!
                val payloadBytes = call.receive<ByteArray>()

                val method = Registry.getLocalService(serviceName)?.methods?.get(methodName)
                    ?: return@post call.respondText(
                        "Service or method not found",
                        status = HttpStatusCode.NotFound
                    )

                val request  = DynamicMessage.parseFrom(method.requestType, payloadBytes)
                val response = method.handler(request)
                call.respondBytes(response.toByteArray())
            }
        }
    }.start(wait = true)
}