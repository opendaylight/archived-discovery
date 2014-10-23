/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.state;

import java.math.BigInteger;
import java.util.Date;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.discovery.NEID;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.provider.rev140714.IdentifyNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.provider.rev140714.DiscoveryIdentificationProviderListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.DiscoveryIdentificationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementIdentified;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementInProcess;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.UnableToIdentifyNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.State;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.discovery.states.DiscoveryState;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.discovery.states.DiscoveryStateBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.discovery.states.DiscoveryStateKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.state.rev140714.DiscoveryStateChangeBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.DiscoverySynchronizationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementSynchronizationFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementSynchronized;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.CheckedFuture;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to network elements being identified. If a
 * network element is identified then the handler queues up a request to synchronize the network element. If a network
 * element fails to be identified then an error message is logged.
 *
 * @author David Bainbridge <dbainbri@ciena.com>
 * @since 2014-07-15
 */
public class StateProvider implements DiscoveryListener, DiscoveryIdentificationProviderListener,
        DiscoveryIdentificationListener, DiscoverySynchronizationListener, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(StateProvider.class);
    private final NotificationProviderService notificationProviderService;
    private final DataBroker dataBroker;

    public StateProvider(NotificationProviderService notificationProviderService, DataBroker dataBroker) {
        this.notificationProviderService = notificationProviderService;
        this.dataBroker = dataBroker;
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

    private synchronized void updateState(DiscoveryStateChangeBuilder builder) {
        NEID neId = NEID.fromParts(builder.getNetworkElementIp(), builder.getNetworkElementType());
        DiscoveryStateBuilder state = new DiscoveryStateBuilder();
        state.setId(neId.getValue());
        state.setState(builder.getToState());
        state.setTimestamp(builder.getTimestamp());

        final InstanceIdentifierBuilder<DiscoveryState> id = InstanceIdentifier.builder(DiscoveryStates.class).child(
                DiscoveryState.class, new DiscoveryStateKey(neId.getValue()));
        final ReadWriteTransaction wo = dataBroker.newReadWriteTransaction();
        wo.merge(LogicalDatastoreType.OPERATIONAL, id.build(), state.build());
        CheckedFuture<Void, TransactionCommitFailedException> sync = wo.submit();
        try {
            sync.checkedGet();
        } catch (TransactionCommitFailedException e) {
            log.error(String.format("Error while attempting to persist status update for request {}, to status {}",
                    builder.getRequestId(), builder.getToState()), e);
        }
        log.debug("STORE : PUT : /discovery-states/discovery-state/{} : {}", neId.getValue(), builder.getToState());
        log.debug("TRANSITION: from {} to {} @ {}", builder.getFromState(), builder.getToState(),
                builder.getTimestamp());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryListener#onNewNetworkElement(
     * NewNetworkElement)
     */
    @Override
    public void onNewNetworkElement(NewNetworkElement notification) {
        /*
         * This notification is not handle by the state manager because it is the notification that initiates the
         * process, thus it is the initial state and will be handled by the identity manager. This eliminates a race
         * condition between the state manager and the identity manager as to which reads and / or writes the the
         * initial state in the data store.
         */
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.provider.rev140714.
     * IdentificationProviderListener
     * #onIdentifyNetworkElement(org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification
     * .provider.rev140714. IdentifyNetworkElement)
     */
    @Override
    public void onIdentifyNetworkElement(IdentifyNetworkElement notification) {
        DiscoveryStateChangeBuilder builder = new DiscoveryStateChangeBuilder();
        builder.setFromState(State.Unidentified);
        builder.setToState(State.Identifying);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : DiscoveryStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.IdentificationListener#
     * onNetworkElementIdentified
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementIdentified)
     */
    @Override
    public void onNetworkElementIdentified(NetworkElementIdentified notification) {
        /*
         * This will be a no-op for status updates as this notification is really the entry point into the discovery
         * system. This notification will be handled by the identity handler.
         */
        DiscoveryStateChangeBuilder builder = new DiscoveryStateChangeBuilder();
        builder.setFromState(State.Identifying);
        builder.setToState(State.Synchronizing);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : DiscoveryStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.ciena.discovery.identification.rev140714.IdentificationListener#
     * onNetworkElementInProcess
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementInProcess)
     */
    @Override
    public void onNetworkElementInProcess(NetworkElementInProcess notification) {
        // no op
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.IdentificationListener#
     * onUnableToIdentifyNetworkElement
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.UnableToIdentifyNetworkElement)
     */
    @Override
    public void onUnableToIdentifyNetworkElement(UnableToIdentifyNetworkElement notification) {
        DiscoveryStateChangeBuilder builder = new DiscoveryStateChangeBuilder();
        builder.setFromState(State.Identifying);
        builder.setToState(State.IdentificationFailed);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : DiscoveryStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.SynchronizationListener#
     * onNetworkElementSynchronizationFailure
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714
     * .NetworkElementSynchronizationFailure)
     */
    @Override
    public void onNetworkElementSynchronizationFailure(NetworkElementSynchronizationFailure notification) {
        DiscoveryStateChangeBuilder builder = new DiscoveryStateChangeBuilder();
        builder.setFromState(State.Synchronizing);
        builder.setToState(State.SynchronizationFailed);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : DiscoveryStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.SynchronizationListener#
     * onNetworkElementedSynchronized
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementedSynchronized)
     */
    @Override
    public void onNetworkElementSynchronized(NetworkElementSynchronized notification) {
        DiscoveryStateChangeBuilder builder = new DiscoveryStateChangeBuilder();
        builder.setFromState(State.Synchronizing);
        builder.setToState(State.Discovered);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : DiscoveryStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }
}
