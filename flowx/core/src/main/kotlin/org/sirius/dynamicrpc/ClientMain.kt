package org.sirius.dynamicrpc

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val client = RpcClient("http://localhost:8080")

    val userService: UserService = RpcClientProxy.create(client, UserService::class)

    // Looks like a plain local call, goes over RPC under the hood
    val user = userService.getUser(42)
    println("Got user: id=${user.id}, name=${user.name}")

    val greeting = userService.greet("Alice")
    println("Got greeting: $greeting")

    //

    val response = client
        .service("UserService")
        .method("getUser")
        .param("id", 42)
        .invoke()

    println("Response: ")
    response.allFields.forEach { (field, value) ->
        println(" - ${field.name}: $value")
    }

    val greetResp = client
        .service("UserService")
        .method("greet")
        .param("name", "Alice")
        .invoke()

    println("Greet Response:")
    greetResp.allFields.forEach { (field, value) ->
        println(" - ${field.name}: $value")
    }
}