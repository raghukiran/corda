package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.NamedByHash
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FetchDataFlow.DownloadedVsRequestedDataMismatch
import net.corda.core.flows.FetchDataFlow.HashNotFound
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.unwrap
import java.util.*

/**
 * An abstract flow for fetching typed data from a remote peer.
 *
 * Given a set of hashes (IDs), either loads them from local disk or asks the remote peer to provide them.
 *
 * A malicious response in which the data provided by the remote peer does not hash to the requested hash results in
 * [DownloadedVsRequestedDataMismatch] being thrown. If the remote peer doesn't have an entry, it results in a
 * [HashNotFound] exception being thrown.
 *
 * By default this class does not insert data into any local database, if you want to do that after missing items were
 * fetched then override [maybeWriteToDisk]. You *must* override [load]. If the wire type is not the same as the
 * ultimate type, you must also override [convert].
 *
 * @param T The ultimate type of the data being fetched.
 * @param W The wire type of the data being fetched, for when it isn't the same as the ultimate type.
 */
abstract class FetchDataFlow<T : NamedByHash, W : Any>(
        protected val requests: Set<SecureHash>,
        protected val otherSide: Party,
        protected val wrapperType: Class<W>) : FlowLogic<FetchDataFlow.Result<T>>() {

    @CordaSerializable
    class DownloadedVsRequestedDataMismatch(val requested: SecureHash, val got: SecureHash) : IllegalArgumentException()

    @CordaSerializable
    class DownloadedVsRequestedSizeMismatch(val requested: Int, val got: Int) : IllegalArgumentException()

    class HashNotFound(val requested: SecureHash) : FlowException()

    @CordaSerializable
    data class Request(val hashes: List<SecureHash>)

    @CordaSerializable
    data class Result<out T : NamedByHash>(val fromDisk: List<T>, val downloaded: List<T>)

    @Suspendable
    @Throws(HashNotFound::class)
    override fun call(): Result<T> {
        // Load the items we have from disk and figure out which we're missing.
        val (fromDisk, toFetch) = loadWhatWeHave()

        return if (toFetch.isEmpty()) {
            Result(fromDisk, emptyList())
        } else {
            logger.info("Requesting ${toFetch.size} dependency(s) for verification from ${otherSide.name}")

            // TODO: Support "large message" response streaming so response sizes are not limited by RAM.
            // We can then switch to requesting items in large batches to minimise the latency penalty.
            // This is blocked by bugs ARTEMIS-1278 and ARTEMIS-1279. For now we limit attachments and txns to 10mb each
            // and don't request items in batch, which is a performance loss, but works around the issue. We have
            // configured Artemis to not fragment messages up to 10mb so we can send 10mb messages without problems.
            // Above that, we start losing authentication data on the message fragments and take exceptions in the
            // network layer.
            val maybeItems = ArrayList<W>(toFetch.size)
            send(otherSide, Request(toFetch))
            for (hash in toFetch) {
                // We skip the validation here (with unwrap { it }) because we will do it below in validateFetchResponse.
                // The only thing checked is the object type. It is a protocol violation to send results out of order.
                maybeItems += receive(wrapperType, otherSide).unwrap { it }
            }
            // Check for a buggy/malicious peer answering with something that we didn't ask for.
            val downloaded = validateFetchResponse(UntrustworthyData(maybeItems), toFetch)
            logger.info("Fetched ${downloaded.size} elements from ${otherSide.name}")
            maybeWriteToDisk(downloaded)
            Result(fromDisk, downloaded)
        }
    }

    protected open fun maybeWriteToDisk(downloaded: List<T>) {
        // Do nothing by default.
    }

    private fun loadWhatWeHave(): Pair<List<T>, List<SecureHash>> {
        val fromDisk = ArrayList<T>()
        val toFetch = ArrayList<SecureHash>()
        for (txid in requests) {
            val stx = load(txid)
            if (stx == null)
                toFetch += txid
            else
                fromDisk += stx
        }
        return Pair(fromDisk, toFetch)
    }

    protected abstract fun load(txid: SecureHash): T?

    @Suppress("UNCHECKED_CAST")
    protected open fun convert(wire: W): T = wire as T

    private fun validateFetchResponse(maybeItems: UntrustworthyData<ArrayList<W>>,
                                      requests: List<SecureHash>): List<T> {
        return maybeItems.unwrap { response ->
            if (response.size != requests.size)
                throw DownloadedVsRequestedSizeMismatch(requests.size, response.size)
            val answers = response.map { convert(it) }
            // Check transactions actually hash to what we requested, if this fails the remote node
            // is a malicious flow violator or buggy.
            for ((index, item) in answers.withIndex()) {
                if (item.id != requests[index])
                    throw DownloadedVsRequestedDataMismatch(requests[index], item.id)
            }
            answers
        }
    }
}
