package org.sirius.dynamicrpc

import kotlinx.serialization.Serializable

@Serializable
data class FieldInfo(
    val name: String,
    val type: String
)

@Serializable
data class TypeInfo(
    val name: String,
    val fields: List<FieldInfo>
)

@Serializable
data class MethodInfo(
    val name: String,
    val requestType: String,
    val responseType: String
)

@Serializable
data class ServiceInfo(
    val name: String,
    val methods: List<MethodInfo>
)

@Serializable
data class NodeInfo(
    val url: String,
    val services: List<ServiceInfo>,
    val types: List<TypeInfo>
)