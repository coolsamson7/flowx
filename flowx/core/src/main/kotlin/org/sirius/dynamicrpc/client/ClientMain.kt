package org.sirius.dynamicrpc.client
import kotlinx.coroutines.runBlocking
import org.sirius.dynamicrpc.ClientNode
import org.sirius.dynamicrpc.user.UserService

fun main() {
    val node = ClientNode()
    node.start()

    // Proxy style — looks like a local call
    val userService = node.proxy(UserService::class.java)

    val user = userService.getUser(42)
    println("Got user:     id=${user.id}, name=${user.name}")

    val greeting = userService.greet("Alice")
    println("Got greeting: $greeting")

    // Fluent style — when you need dynamic invocation

    runBlocking {
        val resp = node.rpcClient
            .service("UserService")
            .method("getUser")
            .param("id", 7)
            .invoke()
        resp.allFields.forEach { (f, v) -> println("${f.name} = $v") }
    }
}