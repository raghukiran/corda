package net.corda.core.transactions

import net.corda.core.contracts.*
import net.corda.core.crypto.DigitalSignature
import net.corda.core.crypto.MerkleTree
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.keys
import net.corda.core.identity.Party
import net.corda.core.indexOfOrThrow
import net.corda.core.internal.Emoji
import net.corda.core.node.ServicesForResolution
import java.security.PublicKey
import java.security.SignatureException
import java.util.function.Predicate

/**
 * A transaction ready for serialisation, without any signatures attached. A WireTransaction is usually wrapped
 * by a [SignedTransaction] that carries the signatures over this payload.
 * The identity of the transaction is the Merkle tree root of its components (see [MerkleTree]).
 */
class WireTransaction(
        /** Pointers to the input states on the ledger, identified by (tx identity hash, output index). */
        override val inputs: List<StateRef>,
        /** Hashes of the ZIP/JAR files that are needed to interpret the contents of this wire transaction. */
        override val attachments: List<SecureHash>,
        outputs: List<TransactionState<ContractState>>,
        /** Ordered list of ([CommandData], [PublicKey]) pairs that instruct the contracts what to do. */
        override val commands: List<Command>,
        notary: Party?,
        signers: List<PublicKey>,
        type: TransactionType,
        timeWindow: TimeWindow?
) : BaseTransaction(inputs, outputs, notary, signers, type, timeWindow), TraversableTransaction {
    init {
        checkInvariants()
    }

    /** The transaction id is represented by the root hash of Merkle tree over the transaction components. */
    override val id: SecureHash by lazy { merkleTree.hash }

    /** Returns a [StateAndRef] for the given output index. */
    @Suppress("UNCHECKED_CAST")
    fun <T : ContractState> outRef(index: Int): StateAndRef<T> {
        require(index >= 0 && index < outputs.size)
        return StateAndRef(outputs[index] as TransactionState<T>, StateRef(id, index))
    }

    /** Returns a [StateAndRef] for the requested output state, or throws [IllegalArgumentException] if not found. */
    fun <T : ContractState> outRef(state: ContractState): StateAndRef<T> = outRef(outputs.map { it.data }.indexOfOrThrow(state))

    /**
     * Looks up identities and attachments from storage to generate a [LedgerTransaction]. A transaction is expected to
     * have been fully resolved using the resolution flow by this point.
     *
     * @throws AttachmentResolutionException if a required attachment was not found in storage.
     * @throws TransactionResolutionException if an input points to a transaction not found in storage.
     */
    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class)
    fun toLedgerTransaction(services: ServicesForResolution): LedgerTransaction {
        return toLedgerTransaction(
                resolveIdentity = { services.identityService.partyFromKey(it) },
                resolveAttachment = { services.attachments.openAttachment(it) },
                resolveStateRef = { services.loadState(it) }
        )
    }

    /**
     * Looks up identities, attachments and dependent input states using the provided lookup functions in order to
     * construct a [LedgerTransaction]. Note that identity lookup failure does *not* cause an exception to be thrown.
     *
     * @throws AttachmentResolutionException if a required attachment was not found using [resolveAttachment].
     * @throws TransactionResolutionException if an input was not found not using [resolveStateRef].
     */
    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class)
    fun toLedgerTransaction(
            resolveIdentity: (PublicKey) -> Party?,
            resolveAttachment: (SecureHash) -> Attachment?,
            resolveStateRef: (StateRef) -> TransactionState<*>?
    ): LedgerTransaction {
        // Look up public keys to authenticated identities. This is just a stub placeholder and will all change in future.
        val authenticatedArgs = commands.map {
            val parties = it.signers.mapNotNull { pk -> resolveIdentity(pk) }
            AuthenticatedObject(it.signers, parties, it.value)
        }
        // Open attachments specified in this transaction. If we haven't downloaded them, we fail.
        val attachments = attachments.map { resolveAttachment(it) ?: throw AttachmentResolutionException(it) }
        val resolvedInputs = inputs.map { ref ->
            resolveStateRef(ref)?.let { StateAndRef(it, ref) } ?: throw TransactionResolutionException(ref.txhash)
        }
        return LedgerTransaction(resolvedInputs, outputs, authenticatedArgs, attachments, id, notary, mustSign, timeWindow, type)
    }

    /**
     * Build filtered transaction using provided filtering functions.
     */
    fun buildFilteredTransaction(filtering: Predicate<Any>): FilteredTransaction {
        return FilteredTransaction.buildMerkleTransaction(this, filtering)
    }

    /**
     * Builds whole Merkle tree for a transaction.
     */
    val merkleTree: MerkleTree by lazy { MerkleTree.getMerkleTree(availableComponentHashes) }

    /**
     * Construction of partial transaction from WireTransaction based on filtering.
     * @param filtering filtering over the whole WireTransaction
     * @returns FilteredLeaves used in PartialMerkleTree calculation and verification.
     */
    fun filterWithFun(filtering: Predicate<Any>): FilteredLeaves {
        fun notNullFalse(elem: Any?): Any? = if (elem == null || !filtering.test(elem)) null else elem
        return FilteredLeaves(
                inputs.filter { filtering.test(it) },
                attachments.filter { filtering.test(it) },
                outputs.filter { filtering.test(it) },
                commands.filter { filtering.test(it) },
                notNullFalse(notary) as Party?,
                mustSign.filter { filtering.test(it) },
                notNullFalse(type) as TransactionType?,
                notNullFalse(timeWindow) as TimeWindow?
        )
    }

    /**
     * Checks that the given signature matches one of the commands and that it is a correct signature over the tx.
     *
     * @throws SignatureException if the signature didn't match the transaction contents.
     * @throws IllegalArgumentException if the signature key doesn't appear in any command.
     */
    fun checkSignature(sig: DigitalSignature.WithKey) {
        require(commands.any { it.signers.any { sig.by in it.keys } }) { "Signature key doesn't match any command" }
        sig.verify(id)
    }

    override fun toString(): String {
        val buf = StringBuilder()
        buf.appendln("Transaction:")
        for (input in inputs) buf.appendln("${Emoji.rightArrow}INPUT:      $input")
        for (output in outputs) buf.appendln("${Emoji.leftArrow}OUTPUT:     ${output.data}")
        for (command in commands) buf.appendln("${Emoji.diamond}COMMAND:    $command")
        for (attachment in attachments) buf.appendln("${Emoji.paperclip}ATTACHMENT: $attachment")
        return buf.toString()
    }

    // TODO: When Kotlin 1.1 comes out we can make this class a data class again, and have these be autogenerated.

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other?.javaClass != javaClass) return false
        if (!super.equals(other)) return false

        other as WireTransaction

        if (inputs != other.inputs) return false
        if (attachments != other.attachments) return false
        if (outputs != other.outputs) return false
        if (commands != other.commands) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + inputs.hashCode()
        result = 31 * result + attachments.hashCode()
        result = 31 * result + outputs.hashCode()
        result = 31 * result + commands.hashCode()
        return result
    }
}
