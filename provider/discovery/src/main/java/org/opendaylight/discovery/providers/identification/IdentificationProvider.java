/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.identification;

import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Future;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.discovery.NEID;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.provider.rev140714.IdentifyNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementIdentifiedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementInProcessBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoverNetworkElementOutputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElementBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.State;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.discovery.states.DiscoveryState;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.discovery.states.DiscoveryStateBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.discovery.states.DiscoveryStateKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.CheckedFuture;
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
    public void onNewNetworkElement(NewNetworkElement notification) {
        log.debug("EVENT : NewNetworkElement : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());

        /*
         * It no request ID was specified with the notification then generate a request ID
         */
        String reqId = notification.getRequestId();
        if (reqId == null || reqId.isEmpty()) {
            reqId = UUID.randomUUID().toString();
        }

        /*
         * Fetch the unique ID for this network element based on the information in the notification as well as get the
         * current discovery state, if it exists.
         *
         * The life cycle for network elements is Identification, Synchronization, and then discovered.
         *
         * The newRequest flag is used to differentiate between a request that was started in an alternate thread or at
         * an earlier time and one that is in the process of being created. This is required because without it we
         * cannot differentiate if this request started else where and thus needs to be shortcutted.
         */
        NEID neId = NEID.fromNetworkElement(notification);

        final InstanceIdentifierBuilder<DiscoveryState> id = InstanceIdentifier.builder(DiscoveryStates.class).child(
                DiscoveryState.class, new DiscoveryStateKey(neId.getValue()));

        State state = null;
        ReadWriteTransaction rw = null;
        boolean alreadyDiscovered = false;
        try {
            rw = dataBroker.newReadWriteTransaction();

            final ListenableFuture<Optional<DiscoveryState>> data = rw.read(LogicalDatastoreType.OPERATIONAL,
                    id.build());
            final Optional<DiscoveryState> result = data.get();
            if (result.isPresent()) {
                /*
                 * If there is already a record in the data store than this network element is already discovered
                 */
                alreadyDiscovered = true;
                state = result.get().getState();
                log.debug("READ state of {} is {}", neId.getValue(), state);
                rw.cancel();
                rw = null;
            } else {
                state = State.Unidentified;
                DiscoveryStateBuilder s = new DiscoveryStateBuilder();
                s.setState(state);
                s.setTimestamp(BigInteger.valueOf(new Date().getTime()));
                s.setId(neId.getValue());
                rw.merge(LogicalDatastoreType.OPERATIONAL, id.build(), s.build());
                CheckedFuture<Void, TransactionCommitFailedException> sync = rw.submit();
                sync.checkedGet();
                rw = null;
            }
        } catch (Exception e) {
            /*
             * We were not able to read or write the state of this network element, therefore the discovery state is not
             * known.
             */
            log.warn("Unable to determine the discovery state of the network element identified by {}"
                    + " because of the error {} : {}, will default in an attempt to discover network element",
                    notification.getNetworkElementIp(), e.getClass().getName(), e.getMessage());
            state = State.Unknown;
            e.printStackTrace();
        } finally {
            if (rw != null) {
                rw.cancel();
            }
        }
        log.debug("STATE {}, alreadyDiscovered {}", state, alreadyDiscovered);

        NetworkElementInProcessBuilder neipBuilder = null;

        /*
         * If this network element has already been discovered, the what happens to it depends on policy, i.e. do we
         * attempt to re-discover it or just drop the request.
         */
        if (alreadyDiscovered) {
            /*
             * For now the policy will be be if it has already been discovered then we will drop the request and send an
             * in process notification.
             *
             * TODO: Implement a policy enforcement point
             */
            neipBuilder = new NetworkElementInProcessBuilder();
            neipBuilder.setCurrentState(state);
            neipBuilder.setNetworkElementIp(notification.getNetworkElementIp());
            neipBuilder.setNetworkElementType(notification.getNetworkElementType());
            neipBuilder.setRequestId(reqId);
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
        log.debug("NOTIFICATION {}", notification);
        if (notification.getNetworkElementType() == null || notification.getNetworkElementType().isEmpty()) {
            /*
             * The element type has not been specified so, so publish an event that should be handled by the device
             * support plugins
             */
            final IdentifyNetworkElementBuilder builder = new IdentifyNetworkElementBuilder();
            builder.setRequestId(reqId);
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
            builder.setRequestId(reqId);
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
