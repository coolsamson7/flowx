package org.sirius.demo

import org.sirius.dynamicrpc.IngressNode
import org.sirius.flowx.FlowxConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Import


@SpringBootApplication
@Import(FlowxConfiguration::class)
class IngressApp() {
    //@EventListener(ApplicationReadyEvent::class) ddd
    //fun onStartup() {
    //    runner.start()
    //}
}

fun main() {
    IngressNode(
        springAppClass = IngressApp::class,
        nodeUrl  = System.getenv("NODE_URL")  ?: "http://node:8080",
        nodePort = System.getenv("NODE_PORT")?.toInt() ?: 8080
    ).start()
}