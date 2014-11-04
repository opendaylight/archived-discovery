/*

 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.deletion;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DeleteNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DiscoveryDeletionListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.FinalizeNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.FinalizeNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeleted;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeletionFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeletionFailureBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.IpToNodeIds;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NodeIdToStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.State;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeIdKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToState;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToStateKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to
 * network elements being identified. If a network element is identified then
 * the handler queues up a request to synchronize the network element. If a
 * network element fails to be identified then an error message is logged.
 *
 * @author Gunjan Patel <gupatel@ciena.com>
 * @since 2014-10-25
 */
public class DeletionProvider implements DiscoveryDeletionListener, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DeletionProvider.class);
    private final ExecutorService executor;
    private final NotificationProviderService notificationProviderService;
    private final RpcProviderRegistry rpcRegistry;
    private final DataBroker dataBroker;

    public DeletionProvider(NotificationProviderService notificationProviderService, RpcProviderRegistry rpcRegistry,
            int threadPoolSize, DataBroker dataBroker) {
        this.notificationProviderService = notificationProviderService;
        this.rpcRegistry = rpcRegistry;
        this.dataBroker = dataBroker;
        log.debug("CREATE : {} : SIZE = {}", Integer.toHexString(this.hashCode()), threadPoolSize);
        executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public void close() throws Exception {
        // TODO Auto-generated method stub

    }

    void deleteWithNodeId(final String reqId, final String ip, final String nodeid, final String usr,
            final String pass, final String neType) {

        final InstanceIdentifierBuilder<NodeIdToState> id = InstanceIdentifier.builder(NodeIdToStates.class).child(
                NodeIdToState.class, new NodeIdToStateKey(nodeid));
        final ReadOnlyTransaction readTx = dataBroker.newReadOnlyTransaction();
        ListenableFuture<Optional<NodeIdToState>> data = readTx.read(LogicalDatastoreType.OPERATIONAL, id.build());
        readTx.close();

        Futures.addCallback(data, new FutureCallback<Optional<NodeIdToState>>() {
            @Override
            public void onSuccess(final Optional<NodeIdToState> result) {
                if (result.isPresent()) {
                    State st = null;
                    st = result.get().getState();
                    String deviceType = result.get().getNetworkElementType();
                    log.debug("READ State of NodeID: {}, is state: {}, and NE-Type: {}", nodeid, st, deviceType);
                    FinalizeNetworkElementBuilder notification = new FinalizeNetworkElementBuilder();
                    notification.setRequestId(reqId);
                    notification.setNetworkElementIp(ip);
                    notification.setNetworkElementType(deviceType);
                    notification.setUsername(usr);
                    notification.setPassword(pass);
                    notification.setNodeId(nodeid);
                    log.debug("EVENT : FinalizeNetworkElement : PUBLISH : {}, {}, {}", notification.getRequestId(),
                            notification.getNetworkElementIp(), notification.getNetworkElementType());
                    notificationProviderService.publish(notification.build());
                } else {
                    final NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                            .setRequestId(reqId).setNetworkElementIp(ip).setNodeId(nodeid)
                            .setNetworkElementType(neType).setCause("Network Element not present in the inventory")
                            .build();
                    log.debug("EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                            event.getNetworkElementIp(), event.getNetworkElementType());
                    notificationProviderService.publish(event);
                    log.error("DELETION : Identifying NE for Deletion. NE-Type not found : {}, {}", reqId, nodeid);
                }

            }

            @Override
            public void onFailure(final Throwable t) {
                // Error during read
                final NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                        .setRequestId(reqId).setNetworkElementIp(ip).setNodeId(nodeid).setNetworkElementType(neType)
                        .setCause("DeletionProvider: Failed to read transaction EVENT").build();
                log.debug("EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                        event.getNetworkElementIp(), event.getNetworkElementType());
                notificationProviderService.publish(event);
                log.error("DeletionProvider: Failed to read transaction EVENT: {}", reqId);
            }
        });
    }

    /*
     * start DeletionJob as soon as DeleteNE notification is raised
     */
    @Override
    public void onDeleteNetworkElement(DeleteNetworkElement notification) {

        final DeleteNetworkElement input = notification;
        /*
         * If no transaction ID was specified with the notification then
         * generate a transaction ID
         */
        String txnId = input.getRequestId();
        if (txnId == null || txnId.isEmpty()) {
            txnId = UUID.randomUUID().toString();
        }
        final String ftxnId = txnId;

        String nid = input.getNodeId();
        // if node-id is not provided then we lookup node-id based on IP address
        if (nid == null || nid.isEmpty()) {
            final InstanceIdentifierBuilder<IpToNodeId> id = InstanceIdentifier.builder(IpToNodeIds.class).child(
                    IpToNodeId.class, new IpToNodeIdKey(input.getNetworkElementIp()));
            final ReadOnlyTransaction readTx = dataBroker.newReadOnlyTransaction();
            ListenableFuture<Optional<IpToNodeId>> data = readTx.read(LogicalDatastoreType.OPERATIONAL, id.build());
            readTx.close();
            Futures.addCallback(data, new FutureCallback<Optional<IpToNodeId>>() {
                @Override
                public void onSuccess(final Optional<IpToNodeId> readResult) {
                    if (readResult.isPresent()) { // check if we have an entry
                                                  // for that IP in
                                                  // IP-to-Node-id table
                        log.debug("READ Node-ID of IP: {}, is Node-ID: {}", input.getNetworkElementIp(), readResult
                                .get().getNodeId().toString());
                        final String fnid = readResult.get().getNodeId();
                        deleteWithNodeId(ftxnId, input.getNetworkElementIp(), fnid, input.getUsername(),
                                input.getPassword(), input.getNetworkElementType());
                    } else {
                        final NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                                .setRequestId(ftxnId)
                                .setNetworkElementIp(input.getNetworkElementIp())
                                .setNodeId(input.getNodeId())
                                .setNetworkElementType(input.getNetworkElementType())
                                .setCause(
                                        "Unable to find node-id for this IP address. Device IP is not in the inventory list")
                                .build();
                        log.debug("EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                                event.getNetworkElementIp(), event.getNetworkElementType());
                        notificationProviderService.publish(event);
                        log.error(
                                "DELETION : Unable to find node-id for this IP address. Device IP is not in the inventory list : {}, {}",
                                input.getRequestId(), input.getNodeId());
                    }
                }

                @Override
                public void onFailure(Throwable arg0) {
                    final NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                            .setRequestId(ftxnId).setNetworkElementIp(input.getNetworkElementIp())
                            .setNodeId(input.getNodeId()).setNetworkElementType(input.getNetworkElementType())
                            .setCause("DeletionProvider: Failed to read transaction EVENT").build();
                    log.debug("EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                            event.getNetworkElementIp(), event.getNetworkElementType());
                    notificationProviderService.publish(event);
                    log.error("DeletionProvider: Failed to read transaction EVENT: {}", input.getRequestId());

                }
            });

        } else {
            deleteWithNodeId(ftxnId, input.getNetworkElementIp(), input.getNodeId(), input.getUsername(),
                    input.getPassword(), input.getNetworkElementType());
        }
    }

    @Override
    public void onNetworkElementDeleted(NetworkElementDeleted notification) {

        final InstanceIdentifierBuilder<NodeIdToState> id = InstanceIdentifier.builder(NodeIdToStates.class).child(
                NodeIdToState.class, new NodeIdToStateKey(notification.getNodeId()));

        final ReadWriteTransaction wo = dataBroker.newReadWriteTransaction();
        wo.delete(LogicalDatastoreType.OPERATIONAL, id.build());

        CheckedFuture<Void, TransactionCommitFailedException> sync = wo.submit();
        try {
            sync.checkedGet();
        } catch (TransactionCommitFailedException e) {
            log.error(String.format(
                    "Error while attempting to delete NE from node-id to state table for request: {}, node-id: {}",
                    notification.getRequestId(), notification.getNodeId(), e));
        }
        log.debug("STORE : DELETE : /node-id-to-states/node-id-to-state/{},  ReqID: {} ", notification.getNodeId(),
                notification.getRequestId());
    }

    @Override
    public void onNetworkElementDeletionFailure(NetworkElementDeletionFailure notification) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onFinalizeNetworkElement(FinalizeNetworkElement notification) {

        log.debug("EVENT : FinalizeNetworkElement Notification : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNodeId());
        executor.submit(new DeletionJob(rpcRegistry, notificationProviderService, notification));
        log.debug("JOB : deletion : SUMMITED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNodeId());

    }

}