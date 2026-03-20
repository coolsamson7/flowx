package org.sirius.demo

import org.sirius.dynamicrpc.RpcType
import org.sirius.dynamicrpc.ServiceNode
import org.sirius.flowx.FlowxConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Import
import org.springframework.stereotype.Component

@SpringBootApplication
@Import(FlowxConfiguration::class)
class NodeApp() {
    /*@EventListener(ApplicationReadyEvent::class)
    fun onStartup() {  s
        runner.start() s
    }*/
}

@RpcType
data class User(val id: Int, val name: String)

@Component
class UserServiceImpl : UserService {
    override fun getUser(id: Int) = User(id, "User-$id")
    override fun greet(name: String) = "Hello, $name!"
}

fun main() {
    ServiceNode(
        springAppClass = NodeApp::class,
        nodeUrl  = System.getenv("NODE_URL")  ?: "http://node:8080",
        nodePort = System.getenv("NODE_PORT")?.toInt() ?: 8080
    ).start()
}