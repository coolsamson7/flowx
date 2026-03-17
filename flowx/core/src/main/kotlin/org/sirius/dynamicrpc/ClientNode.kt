package org.sirius.dynamicrpc

open class ClientNode(
    registryUrl: String = System.getenv("REGISTRY_URL") ?: "http://localhost:9000"
) : RpcNode(registryUrl) {

    override fun nodeName() = "ClientNode"

    override fun start() {
        printLocalState()
        connect()
    }

    /**
     * Create a transparent proxy for the given service interface.
     * Delegates to RpcClientProxy under the hood.
     */
    fun <T : Any> proxy(serviceInterface: Class<T>): T =
        RpcClientProxy.create(rpcClient, serviceInterface.kotlin)
}