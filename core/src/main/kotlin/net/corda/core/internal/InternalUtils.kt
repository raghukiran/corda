package net.corda.core.internal

import com.google.common.base.Throwables
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.sha256
import org.slf4j.Logger
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.lang.reflect.Field
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.*
import java.nio.file.attribute.FileAttribute
import java.time.Duration
import java.time.temporal.Temporal
import java.util.stream.Stream
import java.util.zip.Deflater
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.reflect.KClass

inline val Throwable.rootCause: Throwable get() = Throwables.getRootCause(this)

infix fun Temporal.until(endExclusive: Temporal): Duration = Duration.between(this, endExclusive)

operator fun Duration.div(divider: Long): Duration = dividedBy(divider)
operator fun Duration.times(multiplicand: Long): Duration = multipliedBy(multiplicand)

/**
 * Allows you to write code like: Paths.get("someDir") / "subdir" / "filename" but using the Paths API to avoid platform
 * separator problems.
 */
operator fun Path.div(other: String): Path = resolve(other)
operator fun String.div(other: String): Path = Paths.get(this) / other

fun Path.createDirectory(vararg attrs: FileAttribute<*>): Path = Files.createDirectory(this, *attrs)
fun Path.createDirectories(vararg attrs: FileAttribute<*>): Path = Files.createDirectories(this, *attrs)
fun Path.exists(vararg options: LinkOption): Boolean = Files.exists(this, *options)
fun Path.copyToDirectory(targetDir: Path, vararg options: CopyOption): Path {
    require(targetDir.isDirectory()) { "$targetDir is not a directory" }
    val targetFile = targetDir.resolve(fileName)
    Files.copy(this, targetFile, *options)
    return targetFile
}
fun Path.moveTo(target: Path, vararg options: CopyOption): Path = Files.move(this, target, *options)
fun Path.isRegularFile(vararg options: LinkOption): Boolean = Files.isRegularFile(this, *options)
fun Path.isDirectory(vararg options: LinkOption): Boolean = Files.isDirectory(this, *options)
inline val Path.size: Long get() = Files.size(this)
inline fun <R> Path.list(block: (Stream<Path>) -> R): R = Files.list(this).use(block)
fun Path.deleteIfExists(): Boolean = Files.deleteIfExists(this)
fun Path.readAll(): ByteArray = Files.readAllBytes(this)
inline fun <R> Path.read(vararg options: OpenOption, block: (InputStream) -> R): R = Files.newInputStream(this, *options).use(block)
inline fun Path.write(createDirs: Boolean = false, vararg options: OpenOption = emptyArray(), block: (OutputStream) -> Unit) {
    if (createDirs) {
        normalize().parent?.createDirectories()
    }
    Files.newOutputStream(this, *options).use(block)
}

inline fun <R> Path.readLines(charset: Charset = UTF_8, block: (Stream<String>) -> R): R = Files.lines(this, charset).use(block)
fun Path.readAllLines(charset: Charset = UTF_8): List<String> = Files.readAllLines(this, charset)
fun Path.writeLines(lines: Iterable<CharSequence>, charset: Charset = UTF_8, vararg options: OpenOption): Path = Files.write(this, lines, charset, *options)

fun InputStream.copyTo(target: Path, vararg options: CopyOption): Long = Files.copy(this, target, *options)

fun String.abbreviate(maxWidth: Int): String = if (length <= maxWidth) this else take(maxWidth - 1) + "…"

fun <T> Logger.logElapsedTime(label: String, body: () -> T): T = logElapsedTime(label, this, body)

// TODO: Add inline back when a new Kotlin version is released and check if the java.lang.VerifyError
// returns in the IRSSimulationTest. If not, commit the inline back.
fun <T> logElapsedTime(label: String, logger: Logger? = null, body: () -> T): T {
    // Use nanoTime as it's monotonic.
    val now = System.nanoTime()
    try {
        return body()
    } finally {
        val elapsed = Duration.ofNanos(System.nanoTime() - now).toMillis()
        if (logger != null)
            logger.info("$label took $elapsed msec")
        else
            println("$label took $elapsed msec")
    }
}

/** Convert a [ByteArrayOutputStream] to [InputStreamAndHash]. */
fun ByteArrayOutputStream.toInputStreamAndHash(): InputStreamAndHash {
    val bytes = toByteArray()
    return InputStreamAndHash(ByteArrayInputStream(bytes), bytes.sha256())
}

data class InputStreamAndHash(val inputStream: InputStream, val sha256: SecureHash.SHA256) {
    companion object {
        /**
         * Get a valid InputStream from an in-memory zip as required for some tests. The zip consists of a single file
         * called "z" that contains the given content byte repeated the given number of times.
         * Note that a slightly bigger than numOfExpectedBytes size is expected.
         */
        fun createInMemoryTestZip(numOfExpectedBytes: Int, content: Byte): InputStreamAndHash {
            require(numOfExpectedBytes > 0)
            val baos = ByteArrayOutputStream()
            ZipOutputStream(baos).use { zos ->
                val arraySize = 1024
                val bytes = ByteArray(arraySize) { content }
                val n = (numOfExpectedBytes - 1) / arraySize + 1 // same as Math.ceil(numOfExpectedBytes/arraySize).
                zos.setLevel(Deflater.NO_COMPRESSION)
                zos.putNextEntry(ZipEntry("z"))
                for (i in 0 until n) {
                    zos.write(bytes, 0, arraySize)
                }
                zos.closeEntry()
            }
            return baos.toInputStreamAndHash()
        }
    }
}

/** Returns a [DeclaredField] wrapper around the declared (possibly non-public) static field of the receiver [Class]. */
fun <T> Class<*>.staticField(name: String): DeclaredField<T> = DeclaredField(this, name, null)
/** Returns a [DeclaredField] wrapper around the declared (possibly non-public) static field of the receiver [KClass]. */
fun <T> KClass<*>.staticField(name: String): DeclaredField<T> = DeclaredField(java, name, null)
/** Returns a [DeclaredField] wrapper around the declared (possibly non-public) instance field of the receiver object. */
fun <T> Any.declaredField(name: String): DeclaredField<T> = DeclaredField(javaClass, name, this)
/**
 * Returns a [DeclaredField] wrapper around the (possibly non-public) instance field of the receiver object, but declared
 * in its superclass [clazz].
 */
fun <T> Any.declaredField(clazz: KClass<*>, name: String): DeclaredField<T> = DeclaredField(clazz.java, name, this)

/**
 * A simple wrapper around a [Field] object providing type safe read and write access using [value], ignoring the field's
 * visibility.
 */
class DeclaredField<T>(clazz: Class<*>, name: String, private val receiver: Any?) {
    private val javaField = clazz.getDeclaredField(name).apply { isAccessible = true }
    var value: T
        @Suppress("UNCHECKED_CAST")
        get() = javaField.get(receiver) as T
        set(value) = javaField.set(receiver, value)
}
