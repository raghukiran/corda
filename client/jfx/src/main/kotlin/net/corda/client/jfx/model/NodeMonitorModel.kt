package net.corda.client.jfx.model

import javafx.beans.property.SimpleObjectProperty
import net.corda.client.rpc.CordaRPCClient
import net.corda.client.rpc.CordaRPCClientConfiguration
import net.corda.core.flows.StateMachineRunId
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.StateMachineInfo
import net.corda.core.messaging.StateMachineTransactionMapping
import net.corda.core.messaging.StateMachineUpdate
import net.corda.core.node.services.NetworkMapCache.MapChange
import net.corda.core.node.services.Vault
import net.corda.core.utilities.seconds
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.NetworkHostAndPort
import rx.Observable
import rx.subjects.PublishSubject

data class ProgressTrackingEvent(val stateMachineId: StateMachineRunId, val message: String) {
    companion object {
        fun createStreamFromStateMachineInfo(stateMachine: StateMachineInfo): Observable<ProgressTrackingEvent>? {
            return stateMachine.progressTrackerStepAndUpdates?.let { (current, future) ->
                future.map { ProgressTrackingEvent(stateMachine.id, it) }.startWith(ProgressTrackingEvent(stateMachine.id, current))
            }
        }
    }
}

/**
 * This model exposes raw event streams to and from the node.
 */
class NodeMonitorModel {

    private val stateMachineUpdatesSubject = PublishSubject.create<StateMachineUpdate>()
    private val vaultUpdatesSubject = PublishSubject.create<Vault.Update>()
    private val transactionsSubject = PublishSubject.create<SignedTransaction>()
    private val stateMachineTransactionMappingSubject = PublishSubject.create<StateMachineTransactionMapping>()
    private val progressTrackingSubject = PublishSubject.create<ProgressTrackingEvent>()
    private val networkMapSubject = PublishSubject.create<MapChange>()

    val stateMachineUpdates: Observable<StateMachineUpdate> = stateMachineUpdatesSubject
    val vaultUpdates: Observable<Vault.Update> = vaultUpdatesSubject
    val transactions: Observable<SignedTransaction> = transactionsSubject
    val stateMachineTransactionMapping: Observable<StateMachineTransactionMapping> = stateMachineTransactionMappingSubject
    val progressTracking: Observable<ProgressTrackingEvent> = progressTrackingSubject
    val networkMap: Observable<MapChange> = networkMapSubject

    val proxyObservable = SimpleObjectProperty<CordaRPCOps?>()

    /**
     * Register for updates to/from a given vault.
     * TODO provide an unsubscribe mechanism
     */
    fun register(nodeHostAndPort: NetworkHostAndPort, username: String, password: String, initialiseSerialization: Boolean = true) {
        val client = CordaRPCClient(
                hostAndPort = nodeHostAndPort,
                configuration = CordaRPCClientConfiguration.default.copy(
                        connectionMaxRetryInterval = 10.seconds
                ),
                initialiseSerialization = initialiseSerialization
        )
        val connection = client.start(username, password)
        val proxy = connection.proxy

        val (stateMachines, stateMachineUpdates) = proxy.stateMachinesAndUpdates()
        // Extract the flow tracking stream
        // TODO is there a nicer way of doing this? Stream of streams in general results in code like this...
        val currentProgressTrackerUpdates = stateMachines.mapNotNull { stateMachine ->
            ProgressTrackingEvent.createStreamFromStateMachineInfo(stateMachine)
        }
        val futureProgressTrackerUpdates = stateMachineUpdatesSubject.map { stateMachineUpdate ->
            if (stateMachineUpdate is StateMachineUpdate.Added) {
                ProgressTrackingEvent.createStreamFromStateMachineInfo(stateMachineUpdate.stateMachineInfo) ?: Observable.empty<ProgressTrackingEvent>()
            } else {
                Observable.empty<ProgressTrackingEvent>()
            }
        }

        // We need to retry, because when flow errors, we unsubscribe from progressTrackingSubject. So we end up with stream of state machine updates and no progress trackers.
        futureProgressTrackerUpdates.startWith(currentProgressTrackerUpdates).flatMap { it }.retry().subscribe(progressTrackingSubject)

        // Now the state machines
        val currentStateMachines = stateMachines.map { StateMachineUpdate.Added(it) }
        stateMachineUpdates.startWith(currentStateMachines).subscribe(stateMachineUpdatesSubject)

        // Vault updates
        val (vault, vaultUpdates) = proxy.vaultAndUpdates()
        val initialVaultUpdate = Vault.Update(setOf(), vault.toSet())
        vaultUpdates.startWith(initialVaultUpdate).subscribe(vaultUpdatesSubject)

        // Transactions
        val (transactions, newTransactions) = proxy.verifiedTransactions()
        newTransactions.startWith(transactions).subscribe(transactionsSubject)

        // SM -> TX mapping
        val (smTxMappings, futureSmTxMappings) = proxy.stateMachineRecordedTransactionMapping()
        futureSmTxMappings.startWith(smTxMappings).subscribe(stateMachineTransactionMappingSubject)

        // Parties on network
        val (parties, futurePartyUpdate) = proxy.networkMapUpdates()
        futurePartyUpdate.startWith(parties.map { MapChange.Added(it) }).subscribe(networkMapSubject)

        proxyObservable.set(proxy)
    }
}
