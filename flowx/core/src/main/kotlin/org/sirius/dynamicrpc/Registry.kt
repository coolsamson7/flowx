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

    fun registerType(name: String, descriptor: Descriptors.Descriptor) { types[name] = descriptor }
    fun getTypes(): Map<String, Descriptors.Descriptor> = types

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

    // --- Descriptor helpers ---

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

    /** Build a descriptor from explicit (name, type) pairs — used for request messages. */
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

    /**
     * Build a request descriptor from KParameters (the actual method params, minus 'this').
     * Each parameter becomes a named field with the correct proto type.
     */
    private fun buildRequestDescriptor(messageName: String, params: List<KParameter>): Descriptors.Descriptor {
        val fields = params.map { param ->
            val kClass = param.type.classifier as? KClass<*> ?: String::class
            (param.name ?: "arg${param.index}") to kClass
        }
        return buildDescriptorFromFields(messageName, fields)
    }

    /**
     * Build a response descriptor.
     * - Data classes: use their member properties (e.g. User → {id: INT32, name: STRING})
     * - Scalars/String: wrap in a single-field message named "value"
     */
    private fun buildResponseDescriptor(messageName: String, returnClass: KClass<*>): Descriptors.Descriptor {
        return if (returnClass in SCALAR_TYPES) {
            buildDescriptorFromFields(messageName, listOf("value" to returnClass))
        } else {
            // Register the return type itself first
            val returnTypeName = returnClass.simpleName!!
            if (Registry.getTypes()[returnTypeName] == null) {
                val fields = returnClass.memberProperties.map { prop ->
                    val kClass = prop.returnType.classifier as? KClass<*> ?: String::class
                    prop.name to kClass
                }
                buildDescriptorFromFields(returnTypeName, fields)  // registers "User"
            }
            // Response wrapper just re-uses the same descriptor
            Registry.getTypes()[returnTypeName]!!
        }
    }

    /**
     * Coerce a value extracted from DynamicMessage to the Kotlin type expected by the function.
     * Protobuf returns the correct JVM boxed types for scalar fields, but we need to be explicit
     * for Int since proto returns java.lang.Integer which Kotlin reflection expects as kotlin.Int.
     */
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

    /**
     * Build the field map to construct the response DynamicMessage.
     * - Data classes: reflect over their properties to get values
     * - Scalars/String: put result under the "value" field
     */
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

    fun scanAndRegister(context: ApplicationContext, url: String? = null) {
        val beans = context.getBeansWithAnnotation(RpcService::class.java)
        beans.forEach { (_, bean) ->
            val clazz = bean::class

            // Find the @RpcService-annotated interface, fall back to the class itself
            val (serviceAnnotation, annotatedKClass) = clazz.java.interfaces
                .mapNotNull { iface ->
                    val ann = iface.kotlin.findAnnotation<RpcService>()
                    if (ann != null) ann to iface.kotlin else null
                }
                .firstOrNull()
                ?: ((clazz.findAnnotation<RpcService>() ?: return@forEach) to clazz)

            val serviceName = serviceAnnotation.name.ifEmpty { annotatedKClass.simpleName!! }
            val service = Service(serviceName)

            // Scan @RpcMethod from the INTERFACE, not the impl
            annotatedKClass.memberFunctions.forEach { ifaceFunc ->
                val rpcAnno = ifaceFunc.findAnnotation<RpcMethod>() ?: return@forEach
                val methodName = rpcAnno.name.ifEmpty { ifaceFunc.name }

                // Find the matching impl function on the bean to actually call
                val implFunc = clazz.declaredFunctions.find { it.name == ifaceFunc.name }
                    ?: error("No implementation found for '${ifaceFunc.name}' in ${clazz.simpleName}")

                val funcParams = ifaceFunc.parameters.drop(1)  // param names from interface (guaranteed present)
                val returnClass = ifaceFunc.returnType.classifier as? KClass<*> ?: Any::class

                val requestType  = buildRequestDescriptor("${methodName}Request", funcParams)
                val responseType = buildResponseDescriptor("${methodName}Response", returnClass)

                service.methods[methodName] = ServiceMethod(
                    name = methodName,
                    requestType = requestType,
                    responseType = responseType,
                    handler = { request ->
                        val args = funcParams.map { param ->
                            val field = requestType.findFieldByName(param.name!!)
                                ?: error("No field '${param.name}' in request for '$methodName'")
                            coerceArg(request.getField(field), param.type.classifier as KClass<*>)
                        }
                        // Call the IMPL function on the bean
                        val result = implFunc.call(bean, *args.toTypedArray())
                        val fieldMap = buildResponseFieldMap(responseType, returnClass, result)
                        DynamicProtoGenerator.buildDynamicMessage(responseType, fieldMap)
                    }
                )
            }
            registerLocalService(service, url)
        }
    }
}