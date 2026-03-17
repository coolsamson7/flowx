package org.sirius.dynamicrpc

/*
 * @COPYRIGHT (C) 2023 Andreas Ernst
 *
 * All rights reserved
 */
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
                Int::class -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32
                Long::class -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64
                String::class -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
                Boolean::class -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL
                else -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
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
            .setName("${name}.proto")
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
            val field = descriptor.findFieldByName(k)
            if (field != null && v != null) builder.setField(field, v)
        }
        return builder.build()
    }
}