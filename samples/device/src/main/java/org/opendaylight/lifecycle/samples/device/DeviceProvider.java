/*
 * Copyright (c) 2014 Ciena Corporation and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.lifecycle.samples.device;

import java.net.InetAddress;
import java.util.concurrent.Future;

import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RoutedRpcRegistration;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.provider.rev140714.IdentifyNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.provider.rev140714.LifecycleIdentificationProviderListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementIdentifiedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.LifecycleSynchronizationService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizeNetworkElementInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizeNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizeNetworkElementOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.synchronize.network.element.output.result.SuccessBuilder;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;

/**
 * OSGi/ODL Activator that provides and implementation of the generator interface to generate a given number of network
 * element identified notifications.
 *
 * @author David Bainbridge <dbainbri@ciena.com>
 * @since 2014-07-15
 */
public class DeviceProvider implements DataChangeListener, LifecycleIdentificationProviderListener,
        LifecycleSynchronizationService, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DeviceProvider.class);

    private final NotificationProviderService notificationProviderService;

    RoutedRpcRegistration<LifecycleSynchronizationService> rpcRegistration = null;

    public static final String DEVICE_TYPE_TAG = "SAMPLE";

    public DeviceProvider(NotificationProviderService notificationProviderService) {
        this.notificationProviderService = notificationProviderService;
    }

    @Override
    public void onIdentifyNetworkElement(IdentifyNetworkElement notification) {
        log.debug("EVENT : IdentifyNetworkElement : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());

        /*
         * For the sample device, if the sum of the octets in the IP address are even then we will it will be considered
         * identified by this sample device.
         */
        try {
            InetAddress addr = InetAddress.getByName(notification.getNetworkElementIp());
            byte[] octets = addr.getAddress();
            int sum = 0;
            for (byte b : octets) {
                sum += (int) b;
            }
            if (sum % 2 == 0) {
                NetworkElementIdentifiedBuilder builder = new NetworkElementIdentifiedBuilder();
                builder.setRequestId(notification.getRequestId());
                builder.setNetworkElementIp(addr.getHostAddress());
                builder.setNetworkElementType(DEVICE_TYPE_TAG);
                log.debug("EVENT : NetworkElementIdentified : PUBLISH : {}, {}, {}", builder.getRequestId(),
                        builder.getNetworkElementIp(), builder.getNetworkElementType());
                notificationProviderService.publish(builder.build());
            }
        } catch (Exception e) {
            log.error("ERROR PROCESSING IDENTIFICATION EVENT: {}, {}, {}", notification.getRequestId(), e.getClass()
                    .getName(), e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizationService#
     * synchronizeNetworkElement
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizeNetworkElementInput
     * )
     */
    @Override
    public Future<RpcResult<SynchronizeNetworkElementOutput>> synchronizeNetworkElement(
            SynchronizeNetworkElementInput input) {
        log.debug("RPC : synchronizeNetworkElement : RECEIVED : {}, {}, {}", input.getRequestId(),
                input.getNetworkElementIp(), input.getNetworkElementType());
        SynchronizeNetworkElementOutputBuilder builder = new SynchronizeNetworkElementOutputBuilder();
        builder.setRequestId(input.getRequestId());
        builder.setResult(new SuccessBuilder().setChanges(false).build());
        return Futures.immediateFuture(RpcResultBuilder.<SynchronizeNetworkElementOutput> success(builder.build())
                .build());
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.opendaylight.controller.md.sal.binding.api.DataChangeListener#onDataChanged(org.opendaylight.controller.md
     * .sal.common.api.data.AsyncDataChangeEvent)
     */
    @Override
    public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> event) {
        log.error("GOT DATA CHANGE EVENT");
        // Map<InstanceIdentifier<?>, DataObject> data = event.getUpdatedData();
        // if (data != null && data.values() != null) {
        // for (DataObject dto : data.values()) {
        // if (Device.class.isInstance(dto)) {
        // Device device = (Device) dto;
        // CompositeObjectRegistrationBuilder<SynchronizationService> cbuilder = CompositeObjectRegistration
        // .<SynchronizationService> builderFor(this);
        //
        // NodeUpdatedBuilder builder = new NodeUpdatedBuilder();
        // NodeId nodeId = new NodeId("FOOBAR_001");
        // log.error("NODE ID: " + nodeId);
        // NodeKey nodeKey = new NodeKey(nodeId);
        // InstanceIdentifier<Node> identifier = InstanceIdentifier.builder(Nodes.class)
        // .child(Node.class, nodeKey).toInstance();
        // NodeRef nodeRef = new NodeRef(identifier);
        // RoutedRpcRegistration<SynchronizationService> reg = context.addRoutedRpcImplementation(
        // SynchronizationService.class, this);
        // reg.registerPath(NodeContext.class, identifier);
        // cbuilder.add(reg);
        // builder.setId(nodeId);
        // builder.setNodeRef(nodeRef);
        // notificationService.publish(builder.build());
        // log.error("PUBLISHED");
        // }
        // }
        // }
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
