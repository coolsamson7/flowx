package org.sirius.dynamicrpc

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

@RpcService
interface UserService {
    @RpcMethod
    fun getUser(id: Int): User
    @RpcMethod
    fun greet(name: String): String
}