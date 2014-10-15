/*
 * Copyright (c) 2014 Ciena Corporation and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.lifecycle.providers.identification;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadOnlyTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.lifecycle.NEID;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.provider.rev140714.IdentifyNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementIdentifiedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementInProcessBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.DiscoverNetworkElementInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.DiscoverNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.DiscoverNetworkElementOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.LifecycleListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.LifecycleService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.LifecycleStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.NewNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.NewNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.State;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.lifecycle.states.LifecycleState;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.lifecycle.states.LifecycleStateKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
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
public class IdentificationProvider implements LifecycleListener, LifecycleService, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(IdentificationProvider.class);
    private final NotificationProviderService notificationProviderService;
    private final DataBroker dataBroker;

    public IdentificationProvider(NotificationProviderService notificationProviderService, DataBroker dataBroker) {
        this.notificationProviderService = notificationProviderService;
        this.dataBroker = dataBroker;
    }

    @Override
    public void onNewNetworkElement(NewNetworkElement notification) {
        log.debug("EVENT : NewNetworkElement : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());

        /*
         * It no request ID was specified with the notification then generate a request ID
         */
        String txnId = notification.getRequestId();
        if (txnId == null || txnId.isEmpty()) {
            txnId = UUID.randomUUID().toString();
        }

        /*
         * Fetch the unique ID for this network element based on the information in the notification as well as get the
         * current lifecycle state, if it exists.
         *
         * The life cycle for network elements is Identification, Synchronization, and then discovered.
         *
         * The newRequest flag is used to differentiate between a request that was started in an alternate thread or at
         * an earlier time and one that is in the process of being created. This is required because without it we
         * cannot differentiate if this request started else where and thus needs to be shortcutted.
         */
        NEID neId = NEID.fromNetworkElement(notification);

        final InstanceIdentifierBuilder<LifecycleState> id = InstanceIdentifier.builder(LifecycleStates.class).child(
                LifecycleState.class, new LifecycleStateKey(neId.getValue()));
        final ReadOnlyTransaction ro = dataBroker.newReadOnlyTransaction();
        final ListenableFuture<Optional<LifecycleState>> data = ro.read(LogicalDatastoreType.OPERATIONAL, id.build());
        State state = null;
        try {
            Optional<LifecycleState> result = data.get();
            if (result.isPresent()) {
                state = result.get().getState();
            } else {
                state = State.Unknown;
            }
        } catch (InterruptedException | ExecutionException e) {
            state = State.Unknown;
        }
        ro.close();

        NetworkElementInProcessBuilder neipBuilder = null;
        switch (state) {
        case Unknown:
            /*
             * If the state is unknown that means that this device has not entered the discovery process yet and a
             * record needs to be created and pushed to the store.
             */
            break;
        case Discovered:
            /*
             * If this device has already been discovered, then we really just need to synchronize the data from the
             * network at this point. We do want to look at the timestamp of the last state change because it is
             * possible that it was discovered not too long ago, and if so we may not want to rediscover it quite so
             * soon.
             */
            neipBuilder = new NetworkElementInProcessBuilder();
            neipBuilder.setCurrentState(state);
            neipBuilder.setNetworkElementIp(notification.getNetworkElementIp());
            neipBuilder.setNetworkElementType(notification.getNetworkElementType());
            neipBuilder.setRequestId(txnId);
            log.debug("EVENT : NetworkElementInProcess : PUBLISH : {}, {}, {}, {}", neipBuilder.getRequestId(),
                    neipBuilder.getNetworkElementIp(), neipBuilder.getNetworkElementType(),
                    neipBuilder.getCurrentState());
            notificationProviderService.publish(neipBuilder.build());
            return;
        case DiscoveryFailed:
        case IdentificationFailed:
        case SynchronizationFailed:
            neipBuilder = new NetworkElementInProcessBuilder();
            neipBuilder.setCurrentState(state);
            neipBuilder.setNetworkElementIp(notification.getNetworkElementIp());
            neipBuilder.setNetworkElementType(notification.getNetworkElementType());
            neipBuilder.setRequestId(txnId);
            log.debug("EVENT : NetworkElementInProcess : PUBLISH : {}, {}, {}, {}", neipBuilder.getRequestId(),
                    neipBuilder.getNetworkElementIp(), neipBuilder.getNetworkElementType(),
                    neipBuilder.getCurrentState());
            notificationProviderService.publish(neipBuilder.build());
            return;
        default:
            /*
             * Any other state and we are in the process of discovering the device, so we need to just punt and publish
             * that it is already being processed.
             */
            neipBuilder = new NetworkElementInProcessBuilder();
            neipBuilder.setCurrentState(state);
            neipBuilder.setNetworkElementIp(notification.getNetworkElementIp());
            neipBuilder.setNetworkElementType(notification.getNetworkElementType());
            neipBuilder.setRequestId(txnId);
            log.debug("EVENT : NetworkElementInProcess : PUBLISH : {}, {}, {}, {}", neipBuilder.getRequestId(),
                    neipBuilder.getNetworkElementIp(), neipBuilder.getNetworkElementType(),
                    neipBuilder.getCurrentState());
            notificationProviderService.publish(neipBuilder.build());
            return;
        }

        /*
         * A new network element was recognized on the network. Check if the device has been bound to a node type. If it
         * has been bound then the flow can go directly to synchronization
         */
        if (notification.getNetworkElementType() == null || notification.getNetworkElementType().isEmpty()) {
            /*
             * The element type has not been specified so, so publish an event that should be handled by the device
             * support plugins
             */
            final IdentifyNetworkElementBuilder builder = new IdentifyNetworkElementBuilder();
            builder.setRequestId(txnId);
            builder.setNetworkElementIp(notification.getNetworkElementIp());
            builder.setUsername(notification.getUsername());
            builder.setPassword(notification.getPassword());
            log.debug("EVENT : IdentifyNetworkElement : PUBLISH : {}, {}, {}", builder.getRequestId(),
                    builder.getNetworkElementIp(), builder.getNetworkElementType());

            notificationProviderService.publish(builder.build());
        } else {
            /*
             * The network element is associated with a type, so publish a notification indicating that a network
             * element has been identified. This will be then queued for synchronization.
             */
            final NetworkElementIdentifiedBuilder builder = new NetworkElementIdentifiedBuilder();
            builder.setRequestId(txnId);
            builder.setNetworkElementIp(notification.getNetworkElementIp());
            builder.setUsername(notification.getUsername());
            builder.setPassword(notification.getPassword());
            builder.setNetworkElementType(notification.getNetworkElementType());
            log.debug("EVENT : NetworkElementIdentified : PUBLISH : {}, {}, {}", builder.getRequestId(),
                    builder.getNetworkElementIp(), builder.getNetworkElementType());
            notificationProviderService.publish(builder.build());
        }
    }

    // Translate the RPC into an event and publish that on the channel.
    @Override
    public Future<RpcResult<DiscoverNetworkElementOutput>> discoverNetworkElement(DiscoverNetworkElementInput input) {
        log.debug("RPC: discoverNetworkElement : RECEIVED : {}, {}, {}", input.getRequestId(),
                input.getNetworkElementIp(), input.getNetworkElementType());

        /*
         * It no request ID was specified with the notification then generate a request ID
         */
        String txnId = input.getRequestId();
        if (txnId == null || txnId.isEmpty()) {
            txnId = UUID.randomUUID().toString();
        }

        NewNetworkElementBuilder notification = new NewNetworkElementBuilder();
        notification.setRequestId(txnId);
        notification.setNetworkElementIp(input.getNetworkElementIp());
        notification.setNetworkElementType(input.getNetworkElementType());
        notification.setUsername(input.getUsername());
        notification.setPassword(input.getPassword());
        log.debug("EVENT : NewNetworkElement : PUBLISH : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
        notificationProviderService.publish(notification.build());

        DiscoverNetworkElementOutputBuilder result = new DiscoverNetworkElementOutputBuilder();
        result.setRequestId(txnId);
        result.setResult(true);
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
