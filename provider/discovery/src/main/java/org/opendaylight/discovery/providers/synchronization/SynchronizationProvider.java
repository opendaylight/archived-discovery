/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.synchronization;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.DiscoveryCommunicationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.NetworkElementCommunicationDown;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.NetworkElementCommunicationRestored;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.DiscoveryIdentificationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementIdentified;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementInProcess;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.UnableToIdentifyNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.DiscoverySynchronizationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementSynchronizationFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementSynchronized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronizationProvider implements DiscoveryIdentificationListener, DiscoverySynchronizationListener,
        DiscoveryCommunicationListener, AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(SynchronizationProvider.class);
    private final ExecutorService executor;
    private final NotificationProviderService notificationProviderService;
    private final RpcProviderRegistry rpcRegistry;

    public SynchronizationProvider(NotificationProviderService notificationProviderService,
            RpcProviderRegistry rpcRegistry, int threadPoolSize) {
        this.notificationProviderService = notificationProviderService;
        this.rpcRegistry = rpcRegistry;
        log.debug("CREATE : {} : SIZE = {}", Integer.toHexString(this.hashCode()), threadPoolSize);
        executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public void onNetworkElementIdentified(NetworkElementIdentified notification) {
        /*
         * A new network element has been discovered (existence) and has been identified as a known node type. At this
         * point it needs to be queued up for synchronization.
         */
        log.debug("EVENT : NetworkElementIdentified : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
        executor.submit(new SynchronizationJob(rpcRegistry, notificationProviderService, notification));
        log.debug("JOB : synchronization : SUMMITED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
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
        log.debug("EVENT : UnableToIdentifyNetworkElement : RECEIVED : {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp());
        log.error("Unable to associate network element ({}) with controller node type. CAUSE: {}",
                notification.getNetworkElementIp(), notification.getCause());
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
}
