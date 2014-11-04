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
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DeleteNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.provider.rev140714.IdentifyNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.RemoveNetworkElementInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.RemoveNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.RemoveNetworkElementOutputBuilder;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to
 * network elements being identified. If a network element is identified then
 * the handler queues up a request to synchronize the network element. If a
 * network element fails to be identified then an error message is logged.
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
        builder.setNodeId(null); // node-id is not available when the NE is
                                 // discovered initially
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
        log.debug("RPC: discoverNetworkElement : RECEIVED : {}, {}, {}", input.getRequestId(),
                input.getNetworkElementIp(), input.getNetworkElementType());
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
        notification.setNodeId(null); // node-id is not available when the NE is
                                      // discovered initially
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

    @Override
    public Future<RpcResult<RemoveNetworkElementOutput>> removeNetworkElement(final RemoveNetworkElementInput input) {
        log.debug("RPC: removeNetworkElement : RECEIVED : {}, {}, {}", input.getRequestId(),
                input.getNetworkElementIp(), input.getNetworkElementType());

        /*
         * If no transaction ID was specified with the notification then
         * generate a transaction ID
         */
        String txnId = input.getRequestId();
        if (txnId == null || txnId.isEmpty()) {
            txnId = UUID.randomUUID().toString();
        }

        DeleteNetworkElementBuilder notification = new DeleteNetworkElementBuilder();
        notification.setRequestId(txnId);
        String ip = input.getNetworkElementIp();
        if (ip == null || ip.isEmpty()) {
            ip = null;
        } else {
            notification.setNetworkElementIp(ip);
        }
        notification.setNetworkElementIp(input.getNetworkElementIp());
        notification.setUsername(input.getUsername());
        notification.setPassword(input.getPassword());
        String nid = input.getNodeId();
        if (nid == null || nid.isEmpty()) {
            nid = null;
        } else {
            notification.setNodeId(nid);
        }
        log.debug("EVENT : DeleteNetworkElement : PUBLISH : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
        notificationProviderService.publish(notification.build());

        RemoveNetworkElementOutputBuilder result = new RemoveNetworkElementOutputBuilder();
        result.setRequestId(txnId);
        result.setResult(true);
        return Futures.immediateFuture(RpcResultBuilder.<RemoveNetworkElementOutput> success(result.build()).build());

    }

}
