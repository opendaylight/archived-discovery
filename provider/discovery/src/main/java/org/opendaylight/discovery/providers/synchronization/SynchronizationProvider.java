/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.synchronization;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.DuplicateIdentityBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.DiscoveryCommunicationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.NetworkElementCommunicationDown;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.NetworkElementCommunicationRestored;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.DiscoveryIdentificationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementIdentified;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementInProcess;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.UnableToIdentifyNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.DuplicateRequest;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.DuplicateIdentity;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.IpToNodeIds;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NodeIdToStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.State;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeIdBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeIdKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToState;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToStateBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToStateKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.DiscoverySynchronizationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementSynchronizationFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementSynchronized;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to network elements being identified. If a
 * network element is identified then the handler queues up a request to synchronize the network element. If a network
 * element fails to be identified then an error message is logged.
 *
 * @author David Bainbridge <dbainbri@ciena.com>
 * @since 2014-07-15
 */

public class SynchronizationProvider implements DiscoveryIdentificationListener, DiscoverySynchronizationListener,
DiscoveryCommunicationListener, AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(SynchronizationProvider.class);
    private final ExecutorService executor;
    private final NotificationProviderService notificationProviderService;
    private final RpcProviderRegistry rpcRegistry;
    private final DataBroker dataBroker;

    public SynchronizationProvider(NotificationProviderService notificationProviderService,
            RpcProviderRegistry rpcRegistry, int threadPoolSize, DataBroker dataBroker) {
        this.notificationProviderService = notificationProviderService;
        this.rpcRegistry = rpcRegistry;
        this.dataBroker = dataBroker;
        log.debug("CREATE : {} : SIZE = {}", Integer.toHexString(this.hashCode()), threadPoolSize);
        executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public void onNetworkElementIdentified(final NetworkElementIdentified notification) {
        /*
         * A new network element has been discovered and has been identified as a known node type. At this point it
         * needs to be queued up for synchronization.
         */

        InstanceIdentifierBuilder<IpToNodeId> id1 = InstanceIdentifier.builder(IpToNodeIds.class).child(
                IpToNodeId.class, new IpToNodeIdKey(notification.getNetworkElementIp()));

        IpToNodeIdBuilder table = new IpToNodeIdBuilder();
        table.setNetworkElementIp(notification.getNetworkElementIp());
        table.setNodeId(notification.getNodeId());
        final ReadWriteTransaction wo = dataBroker.newReadWriteTransaction();
        wo.merge(LogicalDatastoreType.OPERATIONAL, id1.build(), table.build());
        wo.submit();
        log.debug("STORE : PUT : /ip-to-node-ids/ip-to-node-id/{} : {} ", notification.getNetworkElementIp(),
                notification.getNodeId());

        /*
         * 1. Now we have the node-ID available so we will put Node-ID in node-id-to-state table a. check if node-id
         * already has an entry in the table (a1). if the node-id is already in the table then check the associated
         * state - raise a DuplicateIdentity notification (a2).else enter a new entry nodeID--state in the table if it's
         * not in the table and send a syncNE routed_RPC
         */
        final InstanceIdentifierBuilder<NodeIdToState> id = InstanceIdentifier.builder(NodeIdToStates.class).child(
                NodeIdToState.class, new NodeIdToStateKey(notification.getNodeId()));

        final ReadOnlyTransaction readTx = dataBroker.newReadOnlyTransaction();
        ListenableFuture<Optional<NodeIdToState>> data = readTx.read(LogicalDatastoreType.OPERATIONAL, id.build());
        readTx.close();

        Futures.addCallback(data, new FutureCallback<Optional<NodeIdToState>>() {
            @Override
            public void onSuccess(final Optional<NodeIdToState> result) {

                if (result.isPresent()) {
                    State st = null;
                    st = result.get().getState();
                    log.debug("READ State of NodeID: {}, is state: {} and IP is {}", notification.getNodeId(), st,
                            notification.getNetworkElementIp());

                    final DuplicateIdentityBuilder notify = new DuplicateIdentityBuilder();
                    notify.setNetworkElementIp(notification.getNetworkElementIp());
                    notify.setRequestId(notification.getRequestId());
                    notify.setNodeId(notification.getNodeId());
                    log.debug("EVENT : DuplicateIdentity : PUBLISH : {}, {}", notify.getRequestId(),
                            notify.getNetworkElementIp());
                    notificationProviderService.publish(notify.build());

                } else {
                    NodeIdToStateBuilder table = new NodeIdToStateBuilder();
                    table.setNodeId(notification.getNodeId());
                    table.setState(State.Synchronizing);
                    final ReadWriteTransaction wo = dataBroker.newReadWriteTransaction();
                    wo.merge(LogicalDatastoreType.OPERATIONAL, id.build(), table.build());
                    wo.submit();

                    log.debug("STORE : PUT : /node-id-to-states/node-id-to-state/{} : {} ", notification.getNodeId(),
                            notification.getNetworkElementIp());

                    log.debug("EVENT : NetworkElementIdentified : RECEIVED : {}, {}, {}", notification.getRequestId(),
                            notification.getNetworkElementIp(), notification.getNetworkElementType());
                    executor.submit(new SynchronizationJob(rpcRegistry, notificationProviderService, notification));
                    log.debug("JOB : synchronization : SUMMITED : {}, {}, {}", notification.getRequestId(),
                            notification.getNetworkElementIp(), notification.getNetworkElementType());

                }

            }

            @Override
            public void onFailure(final Throwable t) {
                // Error during read
                log.error("SynchronizationProvider: Failed to read transaction EVENT: {}", notification.getRequestId());
            }

        });
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.IdentificationListener#
     * onNetworkElementInProcess
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementInProcess)
     */
    @Override
    public void onNetworkElementInProcess(NetworkElementInProcess arg0) {
        // no op
    }

    @Override
    public void onUnableToIdentifyNetworkElement(UnableToIdentifyNetworkElement notification) {
        /*
         * This notification can come from multiple device plugins and is not an error, just really them letting
         * everyone know they cannot identify the device type. As such, log to debug the event.
         */
        log.debug("EVENT : UnableToIdentifyNetworkElement : RECEIVED : {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp());
    }

    @Override
    public void onNetworkElementSynchronizationFailure(NetworkElementSynchronizationFailure notification) {
        log.debug("EVENT : NetworkElementSynchronizationFailure : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
        log.error("Error when attempting to synchronize network element {} using request ID {}. CAUSE {}",
                notification.getNetworkElementIp(), notification.getRequestId(), notification.getCause());
    }

    @Override
    public void onNetworkElementSynchronized(NetworkElementSynchronized notification) {
        // no op
    }

    @Override
    public void onNetworkElementCommunicationDown(NetworkElementCommunicationDown notification) {
        /*
         * No op - need this method to support ODL, which can't deal with implementing only some notification handlers
         */
    }

    @Override
    public void onNetworkElementCommunicationRestored(NetworkElementCommunicationRestored notification) {
        /*
         * Network element Communication has been restored (existence), its data needs to be resynchronized with the
         * network element.
         *
         * TODO: We need to put some sort check here about synchronization, so that in a case of flapping communication
         * we don't repeatedly attempt to synchronize a network element.
         */
        log.debug("EVENT : NetworkElementCommunicationRestored : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
        executor.submit(new SynchronizationJob(rpcRegistry, notificationProviderService, notification));
        log.debug("JOB : synchronization : SUMMITED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() throws Exception {
        log.debug("CLOSE : {}", Integer.toHexString(this.hashCode()));
    }

    @Override
    public void onDuplicateIdentity(DuplicateIdentity notification) {
        // no op

    }

    @Override
    public void onDuplicateRequest(DuplicateRequest notification) {
        // no op

    }
}
