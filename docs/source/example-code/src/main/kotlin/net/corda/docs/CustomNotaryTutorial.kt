package net.corda.docs

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.TransactionVerificationException
import net.corda.core.flows.*
import net.corda.core.identity.Party
import net.corda.core.node.PluginServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.node.services.TimeWindowChecker
import net.corda.core.node.services.TrustedAuthorityNotaryService
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.unwrap
import net.corda.node.services.transactions.PersistentUniquenessProvider
import net.corda.node.services.transactions.ValidatingNotaryService
import java.security.SignatureException

// START 1
@CordaService
class MyCustomValidatingNotaryService(override val services: PluginServiceHub) : TrustedAuthorityNotaryService() {
    companion object {
        val type = ValidatingNotaryService.type.getSubType("mycustom")
    }

    override val timeWindowChecker = TimeWindowChecker(services.clock)
    override val uniquenessProvider = PersistentUniquenessProvider()

    override fun createServiceFlow(otherParty: Party, platformVersion: Int): FlowLogic<Void?> {
        return MyValidatingNotaryFlow(otherParty, this)
    }

    override fun start() {}
    override fun stop() {}
}
// END 1

// START 2
class MyValidatingNotaryFlow(otherSide: Party, service: MyCustomValidatingNotaryService) : NotaryFlow.Service(otherSide, service) {
    /**
     * The received transaction is checked for contract-validity, which requires fully resolving it into a
     * [TransactionForVerification], for which the caller also has to to reveal the whole transaction
     * dependency chain.
     */
    @Suspendable
    override fun receiveAndVerifyTx(): TransactionParts {
        try {
            return receiveTransaction<SignedTransaction>(otherSide, verifySignature = false).unwrap {
                it.verifySignaturesExcept(serviceHub.myInfo.notaryIdentity.owningKey)
                val wtx = it.tx
                val ltx = wtx.toLedgerTransaction(serviceHub).apply { verify() }
                processTransaction(ltx)
                TransactionParts(wtx.id, wtx.inputs, wtx.timeWindow)
            }
        } catch(e: Exception) {
            throw when (e) {
                is TransactionVerificationException -> NotaryException(NotaryError.TransactionInvalid(e))
                is SignatureException -> NotaryException(NotaryError.TransactionInvalid(e))
                else -> e
            }
        }
    }

    fun processTransaction(ltx: LedgerTransaction) {
        // Add custom transaction processing logic here
    }
}
// END 2
