package org.sirius.dynamicrpc.user

import org.sirius.dynamicrpc.RpcMethod
import org.sirius.dynamicrpc.RpcService

@RpcService
interface UserService {
    @RpcMethod
    fun getUser(id: Int): User
    @RpcMethod
    fun greet(name: String): String
}