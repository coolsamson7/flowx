package org.sirius.dynamicrpc

import kotlinx.serialization.Serializable

@Serializable
data class NodeRegistration(
    val nodeInfo: NodeInfo,           // full metadata for introspection
    val services: List<RemoteServiceInfo>  // routing info with base64 descriptors
)