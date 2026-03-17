package org.sirius.dynamicrpc

import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // Client doesn't host any services — just discovers and calls
    val client = RpcClient(registryUrl = "http://localhost:9000")

    // Proxy style
    val userService: UserService = RpcClientProxy.create(client, UserService::class)
    println(userService.getUser(42))
    println(userService.greet("Alice"))

    // Fluent style
    val resp = client.service("UserService").method("getUser").param("id", 7).invoke()
    resp.allFields.forEach { (f, v) -> println("${f.name} = $v") }
}