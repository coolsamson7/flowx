package org.sirius.dynamicrpc

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import org.springframework.context.ApplicationContext
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.full.memberProperties

data class ServiceMethod(
    val name: String,
    val requestType: Descriptors.Descriptor,
    val responseType: Descriptors.Descriptor,
    val handler: suspend (DynamicMessage) -> DynamicMessage
)

data class Service(
    val name: String,
    val methods: MutableMap<String, ServiceMethod> = mutableMapOf()
)

object Registry {
    private val localServices  = mutableMapOf<String, Service>()
    private val remoteServices = mutableMapOf<String, RemoteServiceInfo>()
    private val types          = mutableMapOf<String, Descriptors.Descriptor>()

    // --- Type registration ---

    fun registerType(name: String, descriptor: Descriptors.Descriptor) { types[name] = descriptor }
    fun getTypes(): Map<String, Descriptors.Descriptor> = types

    // --- Service registration ---

    fun registerLocalService(service: Service, url: String? = null) {
        localServices[service.name] = service
        if (url != null) {
            remoteServices[service.name] = RemoteServiceInfo(
                name = service.name,
                url = url,
                methods = service.methods.values.map { m ->
                    RemoteMethodInfo(
                        name = m.name,
                        requestTypeName = m.requestType.name,
                        responseTypeName = m.responseType.name,
                        requestDescriptorProto = java.util.Base64.getEncoder()
                            .encodeToString(m.requestType.file.toProto().toByteArray()),
                        responseDescriptorProto = java.util.Base64.getEncoder()
                            .encodeToString(m.responseType.file.toProto().toByteArray())
                    )
                }
            )
        }
    }

    fun getLocalService(name: String): Service? = localServices[name]
    fun getRemoteService(name: String): RemoteServiceInfo? = remoteServices[name]
    fun getAllLocalServices(): Map<String, Service> = localServices
    fun getAllRemoteServices(): Map<String, RemoteServiceInfo> = remoteServices

    fun registerRemoteServices(services: List<RemoteServiceInfo>) {
        services.forEach { remoteServices[it.name] = it }
    }

    // --- NodeInfo snapshot ---

    fun buildNodeInfo(nodeUrl: String): NodeInfo {
        val serviceInfos = localServices.map { (serviceName, service) ->
            ServiceInfo(
                name = serviceName,
                methods = service.methods.map { (methodName, method) ->
                    MethodInfo(
                        name         = methodName,
                        requestType  = method.requestType.name,
                        responseType = method.responseType.name
                    )
                }
            )
        }
        val typeInfos = types.map { (typeName, descriptor) ->
            TypeInfo(
                name   = typeName,
                fields = descriptor.fields.map { field ->
                    FieldInfo(name = field.name, type = field.typeName())
                }
            )
        }
        return NodeInfo(url = nodeUrl, services = serviceInfos, types = typeInfos)
    }

    // --- Remote descriptor hydration ---

    fun hydrateMethods(methodInfo: RemoteMethodInfo) {
        if (getTypes()[methodInfo.requestTypeName] == null) {
            registerType(
                methodInfo.requestTypeName,
                descriptorFromBase64(methodInfo.requestDescriptorProto, methodInfo.requestTypeName)
            )
        }
        if (getTypes()[methodInfo.responseTypeName] == null) {
            registerType(
                methodInfo.responseTypeName,
                descriptorFromBase64(methodInfo.responseDescriptorProto, methodInfo.responseTypeName)
            )
        }
    }

    fun descriptorFromBase64(base64: String, typeName: String): Descriptors.Descriptor {
        val fileProto = DescriptorProtos.FileDescriptorProto.parseFrom(
            java.util.Base64.getDecoder().decode(base64)
        )
        val fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, arrayOf())
        return fileDescriptor.findMessageTypeByName(typeName)
            ?: error("Type '$typeName' not found in descriptor")
    }

    // --- Descriptor builders ---

    private val SCALAR_TYPES = setOf(
        Int::class, Long::class, String::class, Boolean::class, Double::class, Float::class
    )

    private fun protoFieldType(kClass: KClass<*>) = when (kClass) {
        Int::class     -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32
        Long::class    -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64
        Boolean::class -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL
        Double::class  -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE
        Float::class   -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT
        else           -> DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
    }

    private fun buildDescriptorFromFields(
        messageName: String,
        fields: List<Pair<String, KClass<*>>>
    ): Descriptors.Descriptor {
        val msgBuilder = DescriptorProtos.DescriptorProto.newBuilder().setName(messageName)
        fields.forEachIndexed { index, (fieldName, kClass) ->
            msgBuilder.addField(
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName(fieldName)
                    .setNumber(index + 1)
                    .setType(protoFieldType(kClass))
                    .build()
            )
        }
        val fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("$messageName.proto")
            .addMessageType(msgBuilder)
            .build()
        val fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, arrayOf())
        val descriptor = fileDescriptor.findMessageTypeByName(messageName)
        registerType(messageName, descriptor)
        return descriptor
    }

    private fun ensureRpcTypeRegistered(kClass: KClass<*>): Descriptors.Descriptor? {
        if (kClass.findAnnotation<RpcType>() == null) return null
        val typeName = kClass.simpleName ?: return null
        getTypes()[typeName]?.let { return it }
        val fields = kClass.memberProperties.map { prop ->
            val kc = prop.returnType.classifier as? KClass<*> ?: String::class
            prop.name to kc
        }
        return buildDescriptorFromFields(typeName, fields)
    }

    private fun buildRequestDescriptor(messageName: String, params: List<KParameter>): Descriptors.Descriptor {
        // Single @RpcType param — use its descriptor directly
        if (params.size == 1) {
            val kClass = params[0].type.classifier as? KClass<*>
            if (kClass != null) {
                ensureRpcTypeRegistered(kClass)?.let { return it }
            }
        }

        // Mixed or multi-param — build wrapper
        // @RpcType params become MESSAGE fields referencing their own descriptor
        val msgBuilder = DescriptorProtos.DescriptorProto.newBuilder().setName(messageName)
        val dependentFiles = mutableListOf<Descriptors.FileDescriptor>()

        params.forEachIndexed { index, param ->
            val kClass = param.type.classifier as? KClass<*> ?: String::class
            val rpcTypeDescriptor = ensureRpcTypeRegistered(kClass)

            val fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(param.name ?: "arg${param.index}")
                .setNumber(index + 1)

            if (rpcTypeDescriptor != null) {
                fieldBuilder
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(rpcTypeDescriptor.name)
                dependentFiles.add(rpcTypeDescriptor.file)
            } else {
                fieldBuilder.setType(protoFieldType(kClass))
            }
            msgBuilder.addField(fieldBuilder.build())
        }

        val fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("$messageName.proto")
            .addMessageType(msgBuilder)
            .build()
        val fileDescriptor = Descriptors.FileDescriptor.buildFrom(
            fileProto,
            dependentFiles.toTypedArray()   // pass dependent descriptors so MESSAGE refs resolve
        )
        val descriptor = fileDescriptor.findMessageTypeByName(messageName)
        registerType(messageName, descriptor)
        return descriptor
    }

    private fun buildResponseDescriptor(messageName: String, returnClass: KClass<*>): Descriptors.Descriptor {
        return when {
            returnClass in SCALAR_TYPES ->
                buildDescriptorFromFields(messageName, listOf("value" to returnClass))
            else ->
                ensureRpcTypeRegistered(returnClass)
                    ?: run {
                        val fields = returnClass.memberProperties.map { prop ->
                            val kc = prop.returnType.classifier as? KClass<*> ?: String::class
                            prop.name to kc
                        }
                        buildDescriptorFromFields(messageName, fields)
                    }
        }
    }

    private fun coerceArg(value: Any?, targetClass: KClass<*>): Any? {
        if (value == null) return null
        return when (targetClass) {
            Int::class     -> (value as Number).toInt()
            Long::class    -> (value as Number).toLong()
            Float::class   -> (value as Number).toFloat()
            Double::class  -> (value as Number).toDouble()
            Boolean::class -> value as Boolean
            String::class  -> value.toString()
            else           -> value
        }
    }

    private fun buildResponseFieldMap(
        responseType: Descriptors.Descriptor,
        returnClass: KClass<*>,
        result: Any?
    ): Map<String, Any?> {
        if (result == null) return emptyMap()
        return if (returnClass in SCALAR_TYPES) {
            mapOf("value" to result)
        } else {
            returnClass.memberProperties.associate { prop ->
                prop.name to prop.getter.call(result)
            }
        }
    }

    // --- Spring context scan ---

    fun scanAndRegister(context: ApplicationContext, url: String? = null) {
        val beans = context.getBeansWithAnnotation(RpcService::class.java)
        beans.forEach { (_, bean) ->
            val clazz = bean::class

            val (serviceAnnotation, annotatedKClass) = clazz.java.interfaces
                .mapNotNull { iface ->
                    val ann = iface.kotlin.findAnnotation<RpcService>()
                    if (ann != null) ann to iface.kotlin else null
                }
                .firstOrNull()
                ?: ((clazz.findAnnotation<RpcService>() ?: return@forEach) to clazz)

            val serviceName = serviceAnnotation.name.ifEmpty { annotatedKClass.simpleName!! }
            val service = Service(serviceName)

            // Scan @RpcMethod from the interface, call the impl
            annotatedKClass.memberFunctions.forEach { ifaceFunc ->
                val rpcAnno = ifaceFunc.findAnnotation<RpcMethod>() ?: return@forEach
                val methodName = rpcAnno.name.ifEmpty { ifaceFunc.name }

                val implFunc = clazz.declaredFunctions.find { it.name == ifaceFunc.name }
                    ?: error("No implementation for '${ifaceFunc.name}' in ${clazz.simpleName}")

                val funcParams  = ifaceFunc.parameters.drop(1)
                val returnClass = ifaceFunc.returnType.classifier as? KClass<*> ?: Any::class

                val requestType  = buildRequestDescriptor("${methodName}Request", funcParams)
                val responseType = buildResponseDescriptor("${methodName}Response", returnClass)

                service.methods[methodName] = ServiceMethod(
                    name         = methodName,
                    requestType  = requestType,
                    responseType = responseType,
                    handler = { request ->
                        val args = funcParams.map { param ->
                            val field = requestType.findFieldByName(param.name!!)
                                ?: error("No field '${param.name}' in request for '$methodName'")
                            val raw = request.getField(field)
                            // For MESSAGE fields, reconstruct the @RpcType object
                            val kClass = param.type.classifier as KClass<*>
                            if (raw is DynamicMessage) {
                                reconstructRpcType(raw, kClass)
                            } else {
                                coerceArg(raw, kClass)
                            }
                        }
                        val result = implFunc.call(bean, *args.toTypedArray())
                        val fieldMap = buildResponseFieldMap(responseType, returnClass, result)
                        DynamicProtoGenerator.buildDynamicMessage(responseType, fieldMap)
                    }
                )
            }
            registerLocalService(service, url)
        }
    }

    /**
     * Reconstruct an @RpcType instance from a DynamicMessage using its primary constructor.
     */
    private fun reconstructRpcType(message: DynamicMessage, kClass: KClass<*>): Any {
        val constructor = kClass.constructors.first()
        val args = constructor.parameters.map { param ->
            val field = message.descriptorForType.findFieldByName(param.name!!)
                ?: error("No field '${param.name}' in message for ${kClass.simpleName}")
            coerceArg(message.getField(field), param.type.classifier as KClass<*>)
        }
        return constructor.call(*args.toTypedArray())
    }
}