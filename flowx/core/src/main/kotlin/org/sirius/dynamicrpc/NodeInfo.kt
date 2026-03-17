package org.sirius.dynamicrpc

import com.google.protobuf.Descriptors
import kotlinx.serialization.Serializable

@Serializable
data class FieldInfo(
    val name: String,
    val type: String   // e.g. "string", "int32", "bool", "User", "Address"
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

fun Descriptors.FieldDescriptor.typeName(): String = when (type) {
    Descriptors.FieldDescriptor.Type.INT32,
    Descriptors.FieldDescriptor.Type.SINT32,
    Descriptors.FieldDescriptor.Type.SFIXED32  -> "int32"
    Descriptors.FieldDescriptor.Type.INT64,
    Descriptors.FieldDescriptor.Type.SINT64,
    Descriptors.FieldDescriptor.Type.SFIXED64  -> "int64"
    Descriptors.FieldDescriptor.Type.FLOAT     -> "float"
    Descriptors.FieldDescriptor.Type.DOUBLE    -> "double"
    Descriptors.FieldDescriptor.Type.BOOL      -> "bool"
    Descriptors.FieldDescriptor.Type.STRING    -> "string"
    Descriptors.FieldDescriptor.Type.BYTES     -> "bytes"
    Descriptors.FieldDescriptor.Type.MESSAGE   -> messageType.name  // actual type name e.g. "User"
    Descriptors.FieldDescriptor.Type.ENUM      -> enumType.name
    else                                       -> type.name.lowercase()
}