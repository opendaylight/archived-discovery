/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.identification;

import java.util.UUID;
import java.util.concurrent.Future;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DeleteNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeletionFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeletionFailureBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.provider.rev140714.IdentifyNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.IpToNodeIds;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NodeIdToStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.RemoveNetworkElementInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.RemoveNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.RemoveNetworkElementOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.State;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeIdKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToState;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToStateKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to
 * network elements being identified. If a network element is identified then
 * the handler queues up a request to synchronize the network element. If a
 * network element fails to be identified then an error message is logged.
 *
 * @author David Bainbridge <dbainbri@ciena.com>
 * @since 2014-07-15
 */
public class IdentificationProvider implements DiscoveryListener,
DiscoveryService, AutoCloseable {
    private static final Logger log = LoggerFactory
            .getLogger(IdentificationProvider.class);
    private final NotificationProviderService notificationProviderService;
    private final DataBroker dataBroker;

    public IdentificationProvider(
            NotificationProviderService notificationProviderService,
            DataBroker dataBroker) {
        this.notificationProviderService = notificationProviderService;
        this.dataBroker = dataBroker;
    }

    @Override
    public void onNewNetworkElement(final NewNetworkElement notification) {
        log.debug("EVENT: NewNetworkElement : RECEIVED : {}, {}, {}",
                notification.getRequestId(),
                notification.getNetworkElementIp(),
                notification.getNetworkElementType());

        /*
         * It no request ID was specified with the notification then generate a
         * request ID
         */
        String reqId = notification.getRequestId();
        if (reqId == null || reqId.isEmpty()) {
            reqId = UUID.randomUUID().toString();
        }
        final String freqId = reqId;

        final IdentifyNetworkElementBuilder builder = new IdentifyNetworkElementBuilder();
        builder.setRequestId(freqId);
        builder.setNodeId(null); // node-id is not available when the NE is discovered initially
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setUsername(notification.getUsername());
        builder.setPassword(notification.getPassword());
        log.debug("EVENT : IdentifyNetworkElement : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getNodeId());

        notificationProviderService.publish(builder.build());


    }

    // Translate the RPC into an event and publish that on the channel.
    @Override
    public Future<RpcResult<DiscoverNetworkElementOutput>> discoverNetworkElement(
            final DiscoverNetworkElementInput input) {
        log.debug("RPC: discoverNetworkElement : RECEIVED : {}, {}, {}",
                input.getRequestId(), input.getNetworkElementIp(),
                input.getNetworkElementType());
        /*
         * It no request ID was specified with the notification then generate a
         * request ID
         */
        String txnId = input.getRequestId();
        if (txnId == null || txnId.isEmpty()) {
            txnId = UUID.randomUUID().toString();
        }

        final DiscoverNetworkElementOutputBuilder result = new DiscoverNetworkElementOutputBuilder();
        result.setRequestId(txnId);
        result.setResult(true);
        final NewNetworkElementBuilder notification = new NewNetworkElementBuilder();
        notification.setRequestId(txnId);
        notification.setNodeId(null); // node-id is not available when the NE is discovered initially
        notification.setNetworkElementIp(input.getNetworkElementIp());
        notification.setNetworkElementType(input.getNetworkElementType());
        notification.setUsername(input.getUsername());
        notification.setPassword(input.getPassword());
        log.debug("EVENT : NewNetworkElement : PUBLISH : {}, {}, {}",
                notification.getRequestId(),
                notification.getNetworkElementIp(),
                notification.getNetworkElementType());
        notificationProviderService.publish(notification.build());

        return Futures
                .immediateFuture(RpcResultBuilder
                        .<DiscoverNetworkElementOutput> success(result.build())
                        .build());
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() throws Exception {
        // no op
    }

    @Override
    public Future<RpcResult<RemoveNetworkElementOutput>> removeNetworkElement(
            final RemoveNetworkElementInput input) {
        log.debug("RPC: removeNetworkElement : RECEIVED : {}, {}, {}",
                input.getRequestId(), input.getNetworkElementIp(),
                input.getNetworkElementType());

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
        //if node-id is not provided then we lookup node-id based on IP address
        if (nid == null || nid.isEmpty()) {

            final InstanceIdentifierBuilder<IpToNodeId> id = InstanceIdentifier
                    .builder(IpToNodeIds.class).child(IpToNodeId.class,
                            new IpToNodeIdKey(input.getNetworkElementIp()));

            final ReadOnlyTransaction readTx = dataBroker.newReadOnlyTransaction();
            ListenableFuture<Optional<IpToNodeId>> data = readTx.read(
                    LogicalDatastoreType.OPERATIONAL, id.build());
            readTx.close();
            Futures.addCallback(data, new FutureCallback<Optional<IpToNodeId>>() {
                @Override
                public void onSuccess(final Optional<IpToNodeId> readResult) {

                    if (readResult.isPresent()) {     //checck if we have an entry for that IP in IP-to-Node-id table
                        log.debug("READ Node-ID of IP: {}, is Node-ID: {}",
                                input.getNetworkElementIp(), readResult
                                .get().getNodeId().toString());
                        final String fnid = readResult.get().getNodeId();
                        final InstanceIdentifierBuilder<NodeIdToState> id = InstanceIdentifier
                                .builder(NodeIdToStates.class).child(NodeIdToState.class,
                                        new NodeIdToStateKey(fnid));
                        final ReadOnlyTransaction readTx = dataBroker.newReadOnlyTransaction();
                        ListenableFuture<Optional<NodeIdToState>> data = readTx.read(
                                LogicalDatastoreType.OPERATIONAL, id.build());
                        readTx.close();

                        Futures.addCallback(data,
                                new FutureCallback<Optional<NodeIdToState>>() {
                            @Override
                            public void onSuccess(final Optional<NodeIdToState> result) {

                                if (result.isPresent()) {
                                    State st = null;
                                    st = result.get().getState();
                                    String netype = result.get().getNetworkElementType();
                                    log.debug("READ State of NodeID: {}, is state: {}, and NE-Type: {}", input.getNodeId(), st, netype);

                                        DeleteNetworkElementBuilder notification = new DeleteNetworkElementBuilder();
                                        notification.setRequestId(ftxnId);
                                        notification.setNetworkElementIp(input
                                                .getNetworkElementIp());
                                        notification.setNetworkElementType(netype);
                                        notification.setUsername(input.getUsername());
                                        notification.setPassword(input.getPassword());
                                        notification.setNodeId(fnid);
                                        log.debug(
                                                "EVENT : DeleteNetworkElement : PUBLISH : {}, {}, {}",
                                                notification.getRequestId(),
                                                notification.getNetworkElementIp(),
                                                notification.getNetworkElementType());
                                        notificationProviderService.publish(notification.build());


                                } else {
                                    final NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                                    .setRequestId(ftxnId)
                                    .setNetworkElementIp(
                                            input.getNetworkElementIp())
                                            .setNodeId(input.getNodeId())
                                            .setNetworkElementType(
                                                    input.getNetworkElementType())
                                                    .setCause(
                                                            "Unable to find NE-type for this node-id")
                                                            .build();
                                    log.debug(
                                            "EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}",
                                            event.getRequestId(),
                                            event.getNetworkElementIp(),
                                            event.getNetworkElementType());
                                    notificationProviderService.publish(event);
                                    log.error(
                                            "DELETION : IdentifyingNE for Deletion. NE-Type not found : {}, {}",
                                            input.getRequestId(), input.getNodeId());

                                }

                            }

                            @Override
                            public void onFailure(final Throwable t) {
                                // Error during read
                                log.error(
                                        "DeletionProvider: Failed to read transaction EVENT: {}",
                                        input.getRequestId());
                            }

                        });
                    }
                }

                @Override
                public void onFailure(Throwable arg0) {
                     log.error("DeletionProvider: Failed to read transaction EVENT: {}",input.getRequestId());

                }
            });

        } else {

            final InstanceIdentifierBuilder<NodeIdToState> id = InstanceIdentifier
                    .builder(NodeIdToStates.class).child(NodeIdToState.class,
                            new NodeIdToStateKey(nid));
            final ReadOnlyTransaction readTx = dataBroker.newReadOnlyTransaction();
            ListenableFuture<Optional<NodeIdToState>> data = readTx.read(
                    LogicalDatastoreType.OPERATIONAL, id.build());
            readTx.close();

            Futures.addCallback(data,
                    new FutureCallback<Optional<NodeIdToState>>() {
                @Override
                public void onSuccess(final Optional<NodeIdToState> result) {

                    if (result.isPresent()) {
                        State st = null;
                        st = result.get().getState();
                        String netype = result.get()
                                .getNetworkElementType();
                        log.debug(
                                "READ State of NodeID: {}, is state: {}, and NE-Type: {}",
                                input.getNodeId(), st, netype);

                            DeleteNetworkElementBuilder notification = new DeleteNetworkElementBuilder();
                            notification.setRequestId(ftxnId);
                            notification.setNetworkElementIp(input
                                    .getNetworkElementIp());
                            notification.setNetworkElementType(netype);
                            notification.setUsername(input.getUsername());
                            notification.setPassword(input.getPassword());
                            notification.setNodeId(input.getNodeId());
                            log.debug(
                                    "EVENT : DeleteNetworkElement : PUBLISH : {}, {}, {}",
                                    notification.getRequestId(),
                                    notification.getNetworkElementIp(),
                                    notification.getNetworkElementType());
                            notificationProviderService.publish(notification.build());

                    } else {
                        final NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                        .setRequestId(ftxnId)
                        .setNetworkElementIp(
                                input.getNetworkElementIp())
                                .setNodeId(input.getNodeId())
                                .setNetworkElementType(
                                        input.getNetworkElementType())
                                        .setCause("Unable to find NE-type for this node-id")
                                        .build();
                        log.debug(
                                "EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}",
                                event.getRequestId(),
                                event.getNetworkElementIp(),
                                event.getNetworkElementType());
                        notificationProviderService.publish(event);
                        log.error(
                                "DELETION : IdentifyingNE for Deletion. NE-Type not found : {}, {}",
                                input.getRequestId(), input.getNodeId());

                    }

                }

                @Override
                public void onFailure(final Throwable t) {
                    // Error during read
                    log.error(
                            "DeletionProvider: Failed to read transaction EVENT: {}",
                            input.getRequestId());
                }

            });
        }



        RemoveNetworkElementOutputBuilder result = new RemoveNetworkElementOutputBuilder();
        result.setRequestId(txnId);
        result.setResult(true);
        return Futures.immediateFuture(RpcResultBuilder
                .<RemoveNetworkElementOutput> success(result.build()).build());

    }

}
