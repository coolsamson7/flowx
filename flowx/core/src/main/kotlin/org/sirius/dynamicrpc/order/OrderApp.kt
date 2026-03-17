package org.sirius.dynamicrpc.order

import org.sirius.dynamicrpc.ServiceNode
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class OrderApp

data class Order(val id: Int, val amount: Long)

fun main() {
    ServiceNode(
        springAppClass = OrderApp::class,
        nodeUrl  = System.getenv("NODE_URL")  ?: "http://localhost:8080",
        nodePort = System.getenv("NODE_PORT")?.toInt() ?: 8080
    ).start()
}