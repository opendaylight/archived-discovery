/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.lifecycle.providers.state;

import java.math.BigInteger;
import java.util.Date;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.lifecycle.NEID;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.provider.rev140714.IdentifyNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.provider.rev140714.LifecycleIdentificationProviderListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.LifecycleIdentificationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementIdentified;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementInProcess;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.UnableToIdentifyNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.LifecycleListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.LifecycleStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.NewNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.State;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.lifecycle.states.LifecycleState;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.lifecycle.states.LifecycleStateBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.lifecycle.states.LifecycleStateKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.state.rev140714.LifecycleStateChangeBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.LifecycleSynchronizationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.NetworkElementSynchronizationFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.NetworkElementSynchronized;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to network elements being identified. If a
 * network element is identified then the handler queues up a request to synchronize the network element. If a network
 * element fails to be identified then an error message is logged.
 *
 * @author David Bainbridge <dbainbri@ciena.com>
 * @since 2014-07-15
 */
public class StateProvider implements LifecycleListener, LifecycleIdentificationProviderListener,
        LifecycleIdentificationListener, LifecycleSynchronizationListener, AutoCloseable {
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

    private synchronized void updateState(LifecycleStateChangeBuilder builder) {
        NEID neId = NEID.fromParts(builder.getNetworkElementIp(), builder.getNetworkElementType());
        LifecycleStateBuilder state = new LifecycleStateBuilder();
        state.setId(neId.getValue());
        state.setState(builder.getToState());
        state.setTimestamp(builder.getTimestamp());

        final InstanceIdentifierBuilder<LifecycleState> id = InstanceIdentifier.builder(LifecycleStates.class).child(
                LifecycleState.class, new LifecycleStateKey(neId.getValue()));
        final ReadWriteTransaction wo = dataBroker.newReadWriteTransaction();
        wo.put(LogicalDatastoreType.OPERATIONAL, id.build(), state.build());
        wo.submit();
        log.debug("STORE : PUT : /lifecycle-states/lifecycle-state/{} : {}", neId.getValue(), builder.getToState());
        log.debug("TRANSITION: from {} to {} @ {}", builder.getFromState(), builder.getToState(),
                builder.getTimestamp());
    }

    @Override
    public void onNewNetworkElement(NewNetworkElement notification) {
        LifecycleStateChangeBuilder builder = new LifecycleStateChangeBuilder();
        builder.setFromState(State.Unknown);
        builder.setToState(State.Unidentified);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        log.debug("EVENT : LifecycleStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.provider.rev140714.
     * IdentificationProviderListener
     * #onIdentifyNetworkElement(org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification
     * .provider.rev140714. IdentifyNetworkElement)
     */
    @Override
    public void onIdentifyNetworkElement(IdentifyNetworkElement notification) {
        LifecycleStateChangeBuilder builder = new LifecycleStateChangeBuilder();
        builder.setFromState(State.Unidentified);
        builder.setToState(State.Identifying);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : LifeCycleStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.IdentificationListener#
     * onNetworkElementIdentified
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementIdentified)
     */
    @Override
    public void onNetworkElementIdentified(NetworkElementIdentified notification) {
        LifecycleStateChangeBuilder builder = new LifecycleStateChangeBuilder();
        builder.setFromState(State.Identifying);
        builder.setToState(State.Synchronizing);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : LifecycleStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.ciena.lifecycle.identification.rev140714.IdentificationListener#
     * onNetworkElementInProcess
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementInProcess)
     */
    @Override
    public void onNetworkElementInProcess(NetworkElementInProcess notification) {
        // no op
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.IdentificationListener#
     * onUnableToIdentifyNetworkElement
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.UnableToIdentifyNetworkElement)
     */
    @Override
    public void onUnableToIdentifyNetworkElement(UnableToIdentifyNetworkElement notification) {
        LifecycleStateChangeBuilder builder = new LifecycleStateChangeBuilder();
        builder.setFromState(State.Identifying);
        builder.setToState(State.IdentificationFailed);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : LifecycleStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizationListener#
     * onNetworkElementSynchronizationFailure
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714
     * .NetworkElementSynchronizationFailure)
     */
    @Override
    public void onNetworkElementSynchronizationFailure(NetworkElementSynchronizationFailure notification) {
        LifecycleStateChangeBuilder builder = new LifecycleStateChangeBuilder();
        builder.setFromState(State.Synchronizing);
        builder.setToState(State.SynchronizationFailed);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : LifecycleStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizationListener#
     * onNetworkElementedSynchronized
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.NetworkElementedSynchronized)
     */
    @Override
    public void onNetworkElementSynchronized(NetworkElementSynchronized notification) {
        LifecycleStateChangeBuilder builder = new LifecycleStateChangeBuilder();
        builder.setFromState(State.Synchronizing);
        builder.setToState(State.Discovered);
        builder.setNetworkElementIp(notification.getNetworkElementIp());
        builder.setNetworkElementType(notification.getNetworkElementType());
        builder.setTimestamp(BigInteger.valueOf(new Date().getTime()));
        updateState(builder);
        log.debug("EVENT : LifecycleStateChange : PUBLISH : {}, {}, {}, {}", builder.getRequestId(),
                builder.getNetworkElementIp(), builder.getNetworkElementType(), builder.getToState());
        notificationProviderService.publish(builder.build());
    }
}
