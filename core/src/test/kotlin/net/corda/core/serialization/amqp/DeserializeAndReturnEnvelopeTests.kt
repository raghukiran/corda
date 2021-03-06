package net.corda.core.serialization.amqp

import org.junit.Test
import kotlin.test.*

class DeserializeAndReturnEnvelopeTests {

    fun testName() = Thread.currentThread().stackTrace[2].methodName
    inline fun classTestName(clazz: String) = "${this.javaClass.name}\$${testName()}\$$clazz"

    @Test
    fun oneType() {
        data class A(val a: Int, val b: String)

        val a = A(10, "20")

        var factory = SerializerFactory()
        fun serialise(clazz: Any) = SerializationOutput(factory).serialize(clazz)
        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

        assertTrue(obj.obj is A)
        assertEquals(1, obj.envelope.schema.types.size)
        assertEquals(classTestName("A"), obj.envelope.schema.types.first().name)
    }

    @Test
    fun twoTypes() {
        data class A(val a: Int, val b: String)
        data class B(val a: A, val b: Float)

        val b = B(A(10, "20"), 30.0F)

        var factory = SerializerFactory()
        fun serialise(clazz: Any) = SerializationOutput(factory).serialize(clazz)
        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(b))

        assertTrue(obj.obj is B)
        assertEquals(2, obj.envelope.schema.types.size)
        assertNotEquals(null, obj.envelope.schema.types.find { it.name == classTestName("A") })
        assertNotEquals(null, obj.envelope.schema.types.find { it.name == classTestName("B") })
    }
}
