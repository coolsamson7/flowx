package org.sirius.dynamicrpc

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */

import org.springframework.stereotype.Component

@Component
class UserServiceImpl : UserService {
    override fun getUser(id: Int) = User(id, "User-$id")
    override fun greet(name: String) = "Hello, $name!"
}