package net.corda.core.serialization.carpenter

import net.corda.core.serialization.carpenter.test.AmqpCarpenterBase
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.amqp.*

import org.junit.Test
import kotlin.test.assertEquals

class SingleMemberCompositeSchemaToClassCarpenterTests : AmqpCarpenterBase() {
    @Test
    fun singleInteger() {
        @CordaSerializable
        data class A(val a: Int)

        val test = 10
        val a = A(test)
        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

        assert(obj.obj is A)
        val amqpObj = obj.obj as A

        assertEquals(test, amqpObj.a)
        assertEquals(1, obj.envelope.schema.types.size)
        assert(obj.envelope.schema.types[0] is CompositeType)

        val amqpSchema = obj.envelope.schema.types[0] as CompositeType

        assertEquals(1, amqpSchema.fields.size)
        assertEquals("a", amqpSchema.fields[0].name)
        assertEquals("int", amqpSchema.fields[0].type)

        val carpenterSchema = CarpenterSchemas.newInstance()
        amqpSchema.carpenterSchema(carpenterSchemas = carpenterSchema, force = true)

        val aSchema = carpenterSchema.carpenterSchemas.find { it.name == classTestName("A") }!!
        val aBuilder = ClassCarpenter().build(aSchema)
        val p = aBuilder.constructors[0].newInstance(test)

        assertEquals(aBuilder.getMethod("getA").invoke(p), amqpObj.a)
    }

    @Test
    fun singleString() {
        @CordaSerializable
        data class A(val a: String)

        val test = "ten"
        val a = A(test)

        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

        assert(obj.obj is A)
        val amqpObj = obj.obj as A

        assertEquals(test, amqpObj.a)
        assertEquals(1, obj.envelope.schema.types.size)
        assert(obj.envelope.schema.types[0] is CompositeType)

        val amqpSchema = obj.envelope.schema.types[0] as CompositeType
        val carpenterSchema = CarpenterSchemas.newInstance()
        amqpSchema.carpenterSchema(carpenterSchemas = carpenterSchema, force = true)

        val aSchema = carpenterSchema.carpenterSchemas.find { it.name == classTestName("A") }!!
        val aBuilder = ClassCarpenter().build(aSchema)
        val p = aBuilder.constructors[0].newInstance(test)

        assertEquals(aBuilder.getMethod("getA").invoke(p), amqpObj.a)
    }

    @Test
    fun singleLong() {
        @CordaSerializable
        data class A(val a: Long)

        val test = 10L
        val a = A(test)
        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

        assert(obj.obj is A)
        val amqpObj = obj.obj as A

        assertEquals(test, amqpObj.a)
        assertEquals(1, obj.envelope.schema.types.size)
        assert(obj.envelope.schema.types[0] is CompositeType)

        val amqpSchema = obj.envelope.schema.types[0] as CompositeType

        assertEquals(1, amqpSchema.fields.size)
        assertEquals("a", amqpSchema.fields[0].name)
        assertEquals("long", amqpSchema.fields[0].type)

        val carpenterSchema = CarpenterSchemas.newInstance()
        amqpSchema.carpenterSchema(carpenterSchemas = carpenterSchema, force = true)

        val aSchema = carpenterSchema.carpenterSchemas.find { it.name == classTestName("A") }!!
        val aBuilder = ClassCarpenter().build(aSchema)
        val p = aBuilder.constructors[0].newInstance(test)

        assertEquals(aBuilder.getMethod("getA").invoke(p), amqpObj.a)
    }

    @Test
    fun singleShort() {
        @CordaSerializable
        data class A(val a: Short)

        val test = 10.toShort()
        val a = A(test)
        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

        assert(obj.obj is A)
        val amqpObj = obj.obj as A

        assertEquals(test, amqpObj.a)
        assertEquals(1, obj.envelope.schema.types.size)
        assert(obj.envelope.schema.types[0] is CompositeType)

        val amqpSchema = obj.envelope.schema.types[0] as CompositeType

        assertEquals(1, amqpSchema.fields.size)
        assertEquals("a", amqpSchema.fields[0].name)
        assertEquals("short", amqpSchema.fields[0].type)

        val carpenterSchema = CarpenterSchemas.newInstance()
        amqpSchema.carpenterSchema(carpenterSchemas = carpenterSchema, force = true)

        val aSchema = carpenterSchema.carpenterSchemas.find { it.name == classTestName("A") }!!
        val aBuilder = ClassCarpenter().build(aSchema)
        val p = aBuilder.constructors[0].newInstance(test)

        assertEquals(aBuilder.getMethod("getA").invoke(p), amqpObj.a)
    }

    @Test
    fun singleDouble() {
        @CordaSerializable
        data class A(val a: Double)

        val test = 10.0
        val a = A(test)
        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

        assert(obj.obj is A)
        val amqpObj = obj.obj as A

        assertEquals(test, amqpObj.a)
        assertEquals(1, obj.envelope.schema.types.size)
        assert(obj.envelope.schema.types[0] is CompositeType)

        val amqpSchema = obj.envelope.schema.types[0] as CompositeType

        assertEquals(1, amqpSchema.fields.size)
        assertEquals("a", amqpSchema.fields[0].name)
        assertEquals("double", amqpSchema.fields[0].type)

        val carpenterSchema = CarpenterSchemas.newInstance()
        amqpSchema.carpenterSchema(carpenterSchemas = carpenterSchema, force = true)

        val aSchema = carpenterSchema.carpenterSchemas.find { it.name == classTestName("A") }!!
        val aBuilder = ClassCarpenter().build(aSchema)
        val p = aBuilder.constructors[0].newInstance(test)

        assertEquals(aBuilder.getMethod("getA").invoke(p), amqpObj.a)
    }

    @Test
    fun singleFloat() {
        @CordaSerializable
        data class A(val a: Float)

        val test: Float = 10.0F
        val a = A(test)
        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

        assert(obj.obj is A)
        val amqpObj = obj.obj as A

        assertEquals(test, amqpObj.a)
        assertEquals(1, obj.envelope.schema.types.size)
        assert(obj.envelope.schema.types[0] is CompositeType)

        val amqpSchema = obj.envelope.schema.types[0] as CompositeType

        assertEquals(1, amqpSchema.fields.size)
        assertEquals("a", amqpSchema.fields[0].name)
        assertEquals("float", amqpSchema.fields[0].type)

        val carpenterSchema = CarpenterSchemas.newInstance()
        amqpSchema.carpenterSchema(carpenterSchemas = carpenterSchema, force = true)

        val aSchema = carpenterSchema.carpenterSchemas.find { it.name == classTestName("A") }!!
        val aBuilder = ClassCarpenter().build(aSchema)
        val p = aBuilder.constructors[0].newInstance(test)

        assertEquals(aBuilder.getMethod("getA").invoke(p), amqpObj.a)
    }
}
