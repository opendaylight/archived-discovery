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
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.provider.rev140714.IdentifyNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.DuplicateRequestBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.IpToNodeIds;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeIdBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.ip.to.node.ids.IpToNodeIdKey;
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
 * OSGi/ODL Activator that registers handlers for notifications pertaining to network elements being identified. If a
 * network element is identified then the handler queues up a request to synchronize the network element. If a network
 * element fails to be identified then an error message is logged.
 *
 * @author David Bainbridge <dbainbri@ciena.com>
 * @since 2014-07-15
 */
public class IdentificationProvider implements DiscoveryListener, DiscoveryService, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(IdentificationProvider.class);
    private final NotificationProviderService notificationProviderService;
    private final DataBroker dataBroker;

    public IdentificationProvider(NotificationProviderService notificationProviderService, DataBroker dataBroker) {
        this.notificationProviderService = notificationProviderService;
        this.dataBroker = dataBroker;
    }

    @Override
    public void onNewNetworkElement(final NewNetworkElement notification) {
        log.debug("EVENT: NewNetworkElement : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());

        /*
         * It no request ID was specified with the notification then generate a request ID
         */
        String reqId = notification.getRequestId();
        if (reqId == null || reqId.isEmpty()) {
            reqId = UUID.randomUUID().toString();
        }
        final String freqId = reqId;

        final InstanceIdentifierBuilder<IpToNodeId> id = InstanceIdentifier.builder(IpToNodeIds.class).child(
                IpToNodeId.class, new IpToNodeIdKey(notification.getNetworkElementIp()));

        final ReadOnlyTransaction readTx = dataBroker.newReadOnlyTransaction();
        ListenableFuture<Optional<IpToNodeId>> data = readTx.read(LogicalDatastoreType.OPERATIONAL, id.build());
        readTx.close();
        Futures.addCallback(data, new FutureCallback<Optional<IpToNodeId>>() {
            @Override
            public void onSuccess(final Optional<IpToNodeId> readResult) {

                if (readResult.isPresent()) {
                    log.debug("READ Node-ID of IP: {}, is Node-ID: {}", notification.getNetworkElementIp(), readResult
                            .get().getNodeId().toString());
                    log.warn("Duplicate Request For Ip : {}, reqID: {}, nodeID: {}",
                            notification.getNetworkElementIp(), notification.getRequestId(), readResult.get()
                            .getNodeId().toString());
                    final DuplicateRequestBuilder notify = new DuplicateRequestBuilder();
                    notify.setNetworkElementIp(notification.getNetworkElementIp());
                    notify.setRequestId(freqId);
                    notify.setNodeId(readResult.get().getNodeId());
                    log.debug("EVENT : DuplicateRequest : PUBLISH : {}, {}", notify.getRequestId(),
                            notify.getNetworkElementIp());
                    notificationProviderService.publish(notify.build());

                } else {
                    IpToNodeIdBuilder table = new IpToNodeIdBuilder();
                    table.setNetworkElementIp(notification.getNetworkElementIp());
                    table.setNodeId(null);
                    final ReadWriteTransaction wo = dataBroker.newReadWriteTransaction();
                    wo.merge(LogicalDatastoreType.OPERATIONAL, id.build(), table.build());
                    wo.submit();
                    log.debug("STORE : PUT : /ip-to-node-ids/ip-to-node-id/{} : {} ",
                            notification.getNetworkElementIp(), table.getNodeId());

                    /*
                     * The element type has not been specified so, so publish an event that should be handled by the
                     * device support plugins
                     */
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
            }

            @Override
            public void onFailure(final Throwable t) {
                // Error during read
                log.error("IdentficationProvider: Failed to read transaction EVENT: {}", freqId);
            }

        });

    }

    // Translate the RPC into an event and publish that on the channel.
    @Override
    public Future<RpcResult<DiscoverNetworkElementOutput>> discoverNetworkElement(
            final DiscoverNetworkElementInput input) {
        log.debug("RPC: discoverNetworkElement : RECEIVED : {}, {}, {}", input.getRequestId(),
                input.getNetworkElementIp(), input.getNetworkElementType());
        /*
         * It no request ID was specified with the notification then generate a request ID
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
        log.debug("EVENT : NewNetworkElement : PUBLISH : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
        notificationProviderService.publish(notification.build());

        return Futures.immediateFuture(RpcResultBuilder.<DiscoverNetworkElementOutput> success(result.build()).build());
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
}
