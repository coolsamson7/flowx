package org.sirius.dynamicrpc

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */
import kotlinx.serialization.Serializable

@Serializable
data class RemoteMethodInfo(
    val name: String,
    val requestTypeName: String,
    val responseTypeName: String,
    val requestDescriptorProto: String,  // base64-encoded FileDescriptorProto bytes
    val responseDescriptorProto: String
)

@Serializable
data class RemoteServiceInfo(
    val name: String,
    val url: String,
    val methods: List<RemoteMethodInfo>
)