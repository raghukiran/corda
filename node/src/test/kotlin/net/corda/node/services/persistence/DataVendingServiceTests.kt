package net.corda.node.services.persistence

import co.paralleluniverse.fibers.Suspendable
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.Amount
import net.corda.core.contracts.Issued
import net.corda.core.contracts.TransactionType
import net.corda.core.contracts.USD
import net.corda.core.flows.BroadcastTransactionFlow.NotifyTxRequest
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.identity.Party
import net.corda.core.node.services.unconsumedStates
import net.corda.core.transactions.SignedTransaction
import net.corda.node.services.NotifyTransactionHandler
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.MEGA_CORP
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetwork.MockNode
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

/**
 * Tests for the data vending service.
 */
class DataVendingServiceTests {
    lateinit var mockNet: MockNetwork

    @Before
    fun setup() {
        mockNet = MockNetwork()
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()
    }

    @Test
    fun `notify of transaction`() {
        val nodes = mockNet.createSomeNodes(2)
        val vaultServiceNode = nodes.partyNodes[0]
        val registerNode = nodes.partyNodes[1]
        val beneficiary = vaultServiceNode.info.legalIdentity
        val deposit = registerNode.info.legalIdentity.ref(1)
        mockNet.runNetwork()

        // Generate an issuance transaction
        val ptx = TransactionType.General.Builder(null)
        Cash().generateIssue(ptx, Amount(100, Issued(deposit, USD)), beneficiary, DUMMY_NOTARY)

        // Complete the cash transaction, and then manually relay it
        val tx = registerNode.services.signInitialTransaction(ptx)
        vaultServiceNode.database.transaction {
            assertThat(vaultServiceNode.services.vaultService.unconsumedStates<Cash.State>()).isEmpty()

            registerNode.sendNotifyTx(tx, vaultServiceNode)

            // Check the transaction is in the receiving node
            val actual = vaultServiceNode.services.vaultService.unconsumedStates<Cash.State>().singleOrNull()
            val expected = tx.tx.outRef<Cash.State>(0)
            assertEquals(expected, actual)
        }
    }

    /**
     * Test that invalid transactions are rejected.
     */
    @Test
    fun `notify failure`() {
        val nodes = mockNet.createSomeNodes(2)
        val vaultServiceNode = nodes.partyNodes[0]
        val registerNode = nodes.partyNodes[1]
        val beneficiary = vaultServiceNode.info.legalIdentity
        val deposit = MEGA_CORP.ref(1)
        mockNet.runNetwork()

        // Generate an issuance transaction
        val ptx = TransactionType.General.Builder(DUMMY_NOTARY)
        Cash().generateIssue(ptx, Amount(100, Issued(deposit, USD)), beneficiary, DUMMY_NOTARY)

        // The transaction tries issuing MEGA_CORP cash, but we aren't the issuer, so it's invalid
        val tx = registerNode.services.signInitialTransaction(ptx)
        vaultServiceNode.database.transaction {
            assertThat(vaultServiceNode.services.vaultService.unconsumedStates<Cash.State>()).isEmpty()

            registerNode.sendNotifyTx(tx, vaultServiceNode)

            // Check the transaction is not in the receiving node
            assertThat(vaultServiceNode.services.vaultService.unconsumedStates<Cash.State>()).isEmpty()
        }
    }

    private fun MockNode.sendNotifyTx(tx: SignedTransaction, walletServiceNode: MockNode) {
        walletServiceNode.registerInitiatedFlow(InitiateNotifyTxFlow::class.java)
        services.startFlow(NotifyTxFlow(walletServiceNode.info.legalIdentity, tx))
        mockNet.runNetwork()
    }

    @InitiatingFlow
    private class NotifyTxFlow(val otherParty: Party, val stx: SignedTransaction) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() = send(otherParty, NotifyTxRequest(stx))
    }

    @InitiatedBy(NotifyTxFlow::class)
    private class InitiateNotifyTxFlow(val otherParty: Party) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() = subFlow(NotifyTransactionHandler(otherParty))
    }
}
