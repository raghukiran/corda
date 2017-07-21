package net.corda.core.transactions

import net.corda.core.contracts.*
import net.corda.core.crypto.DigitalSignature
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.toBase58String
import net.corda.core.identity.Party
import net.corda.core.node.ServiceHub
import java.security.PublicKey

/**
 * A special transaction for changing the notary of a state. It only needs specifying the state(s) as input(s),
 * old and new notaries. Output states can be computed by applying the notary modification to corresponding inputs
 * on the fly.
 */
data class NotaryChangeWireTransaction(
        override val inputs: List<StateRef>,
        override val notary: Party,
        val newNotary: Party
) : CoreTransaction {
    init {
        check(inputs.isNotEmpty()) { "A notary change transaction must have inputs" }
        checkNoDuplicateInputs()
        check(notary != newNotary) { "The old and new notaries must be different – $newNotary" }
    }

    override val id: SecureHash by lazy { serializedHash(this) }

    fun resolve(services: ServiceHub, sigs: List<DigitalSignature.WithKey>): NotaryChangeLedgerTransaction {
        val resolvedInputs = inputs.map { ref ->
            services.loadState(ref).let { StateAndRef(it, ref) }
        }
        return NotaryChangeLedgerTransaction(resolvedInputs, notary, newNotary, id, sigs)
    }
}

/**
 * A notary change transaction with fully resolved inputs and signatures. In contrast with a regular transaction,
 * signatures are checked against the signers specified by input states' *participants* fields, so full resolution is
 * needed for signature verification.
 */
data class NotaryChangeLedgerTransaction(
        override val inputs: List<StateAndRef<ContractState>>,
        override val notary: Party,
        val newNotary: Party,
        override val id: SecureHash,
        override val sigs: List<DigitalSignature.WithKey>
) : FullTransaction, TransactionWithSignatures {
    init {
        checkInputsHaveSameNotary()
        checkEncumbrances()
    }

    /** We compute the outputs on demand by applying the notary field modification to the inputs */
    override val outputs: List<TransactionState<ContractState>>
        get() = inputs.mapIndexed { pos, (state) ->
            if (state.encumbrance != null) {
                state.copy(notary = newNotary, encumbrance = pos + 1)
            } else state.copy(notary = newNotary)
        }

    override val requiredSigningKeys: Set<PublicKey>
        get() = inputs.flatMap { it.state.data.participants }.map { it.owningKey }.toSet() + notary.owningKey

    override fun getKeyDescriptions(keys: Set<PublicKey>): List<String> {
        return keys.map { it.toBase58String() }
    }

    /**
     * Check that encumbrances have been included in the inputs. The [NotaryChangeFlow] guarantees that an encumbrance
     * will follow its encumbered state in the inputs.
     */
    private fun checkEncumbrances() {
        inputs.forEachIndexed { i, (state, ref) ->
            state.encumbrance?.let {
                val nextIndex = i + 1
                val nextStateIsEncumbrance = {
                    inputs[nextIndex].ref.txhash == ref.txhash && inputs[nextIndex].ref.index == it
                }
                if (nextIndex >= inputs.size || !nextStateIsEncumbrance()) {
                    throw TransactionVerificationException.TransactionMissingEncumbranceException(
                            id,
                            it,
                            TransactionVerificationException.Direction.INPUT)
                }
            }
        }
    }
}