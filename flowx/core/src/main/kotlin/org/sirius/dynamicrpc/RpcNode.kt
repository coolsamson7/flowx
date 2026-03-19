package org.sirius.dynamicrpc

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

abstract class RpcNode(
    val registryUrl: String = System.getenv("REGISTRY_URL") ?: "http://localhost:9000"
) {
    protected val httpClient = HttpClient(CIO) {
        install(ContentNegotiation) { json() }
    }

    // Lazily created — ready after connect()
    lateinit var rpcClient: RpcClient
        private set

    /**
     * Pull all services from the central registry and hydrate
     * remote descriptors into the local Registry.
     */
    protected fun connect() {
        runBlocking {
            try {
                val allServices: List<RemoteServiceInfo> = httpClient
                    .get("$registryUrl/services")
                    .body()

                allServices.forEach { svc ->
                    if (Registry.getLocalService(svc.name) == null) {
                        svc.methods.forEach { Registry.hydrateMethods(it) }
                        Registry.registerRemoteServices(listOf(svc))
                        println("[${nodeName()}] Discovered remote service '${svc.name}' @ ${svc.url}")
                    }
                }
                println("[${nodeName()}] Connected to registry @ $registryUrl " +
                        "(${allServices.size} service(s) available)")
            } catch (e: Exception) {
                println("[${nodeName()}] WARN: Could not reach registry @ $registryUrl — " +
                        "running in standalone mode")
            }
        }
        rpcClient = RpcClient(registryUrl)
    }

    /**
     * Push a set of local services to the central registry.
     */
    protected fun pushToRegistry(registration: NodeRegistration) {
        runBlocking {
            val maxRetries = 10
            val delayMs = 2000L

            repeat(maxRetries) { attempt ->
                try {
                    httpClient.post("$registryUrl/register") {
                        contentType(ContentType.Application.Json)
                        setBody(registration)
                    }

                    println("[${nodeName()}] Registered successfully on attempt ${attempt + 1}")
                    return@runBlocking

                } catch (e: Exception) {
                    println(
                        "[${nodeName()}] WARN: Registry not available (attempt ${attempt + 1}/$maxRetries) — $e"
                    )

                    delay(delayMs)
                }
            }

            println("[${nodeName()}] ERROR: Failed to register after $maxRetries attempts")
        }
    }

    /**
     * Print a summary of everything registered locally.
     */
    protected fun printLocalState() {
        println("\n=== ${nodeName()} ===")
        if (Registry.getAllLocalServices().isEmpty()) {
            println("  (no local services — client-only mode)")
        } else {
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
        }
        println()
    }

    /** Override to give your node a meaningful name in logs */
    open fun nodeName(): String = this::class.simpleName ?: "RpcNode"

    /** Entry point — subclasses implement their startup logic here */
    abstract fun start()
}