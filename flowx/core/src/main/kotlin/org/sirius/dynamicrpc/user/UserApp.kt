package org.sirius.dynamicrpc.user

import org.sirius.dynamicrpc.ServiceNode
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component

@SpringBootApplication
class UserApp

data class User(val id: Int, val name: String)

@Component
class UserServiceImpl : UserService {
    override fun getUser(id: Int) = User(id, "User-$id")
    override fun greet(name: String) = "Hello, $name!"
}

fun main() {
    ServiceNode(
        springAppClass = UserApp::class,
        nodeUrl  = System.getenv("NODE_URL")  ?: "http://localhost:8080",
        nodePort = System.getenv("NODE_PORT")?.toInt() ?: 8080
    ).start()
}