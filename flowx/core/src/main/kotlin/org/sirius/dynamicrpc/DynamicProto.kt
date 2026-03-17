package org.sirius.dynamicrpc

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties

object DynamicProtoGenerator {

    fun generateDescriptor(kclass: KClass<*>): Descriptors.Descriptor {
        val name = kclass.simpleName ?: "Unknown"
        val messageType = DescriptorProtos.DescriptorProto.newBuilder().setName(name)

        kclass.memberProperties.forEachIndexed { index, prop ->
            val fieldType = when (prop.returnType.classifier) {
                Int::class     -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32
                Long::class    -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64
                String::class  -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
                Boolean::class -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL
                else           -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
            }
            messageType.addField(
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName(prop.name)
                    .setNumber(index + 1)
                    .setType(fieldType)
                    .build()
            )
        }

        val fileDescriptorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("$name.proto")
            .addMessageType(messageType)
            .build()

        val fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, arrayOf())
        val descriptor = fileDescriptor.findMessageTypeByName(name)
        Registry.registerType(name, descriptor)
        return descriptor
    }

    fun buildDynamicMessage(descriptor: Descriptors.Descriptor, values: Map<String, Any?>): DynamicMessage {
        val builder = DynamicMessage.newBuilder(descriptor)
        values.forEach { (k, v) ->
            val field = descriptor.findFieldByName(k) ?: return@forEach
            if (v == null) return@forEach

            val coerced = when (field.type) {
                Descriptors.FieldDescriptor.Type.MESSAGE -> {
                    when (v) {
                        // Already a DynamicMessage — use directly
                        is DynamicMessage -> v
                        // An @RpcType object — recurse into its properties
                        else -> {
                            val nestedDescriptor = field.messageType
                            val nestedValues = v::class.memberProperties.associate { prop ->
                                prop.name to prop.getter.call(v)
                            }
                            buildDynamicMessage(nestedDescriptor, nestedValues)
                        }
                    }
                }
                Descriptors.FieldDescriptor.Type.INT32,
                Descriptors.FieldDescriptor.Type.SINT32,
                Descriptors.FieldDescriptor.Type.SFIXED32  -> (v as Number).toInt()
                Descriptors.FieldDescriptor.Type.INT64,
                Descriptors.FieldDescriptor.Type.SINT64,
                Descriptors.FieldDescriptor.Type.SFIXED64  -> (v as Number).toLong()
                Descriptors.FieldDescriptor.Type.FLOAT     -> (v as Number).toFloat()
                Descriptors.FieldDescriptor.Type.DOUBLE    -> (v as Number).toDouble()
                Descriptors.FieldDescriptor.Type.BOOL      -> v as Boolean
                Descriptors.FieldDescriptor.Type.STRING    -> v.toString()
                else -> v
            }
            builder.setField(field, coerced)
        }
        return builder.build()
    }
}