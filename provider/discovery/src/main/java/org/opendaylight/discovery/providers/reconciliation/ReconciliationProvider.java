/*
 * Copyright (c) 2014 Ciena Corporation and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package org.opendaylight.discovery.providers.reconciliation;

import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.DiscoverySynchronizationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementSynchronizationFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.NetworkElementSynchronized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to network elements being synchronized. If a
 * network element is synchronized then the handler will attempt to reconcile any new connections based on the
 * additional data. If an new connection can be created, it is created and a notification is sent.
 *
 * @author David Bainbridge <dbainbri@ciena.com>
 * @since 2014-08-12
 */
public class ReconciliationProvider implements DiscoverySynchronizationListener, AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(ReconciliationProvider.class);

    @Override
    public void onNetworkElementSynchronized(NetworkElementSynchronized notification) {
        /*
         * A new network element has been synchronized. At this point an attempt needs to be made to create new
         * connections based on the added information to the information base.
         *
         * NOTE: Just because the even was received it does not mean the data about the node was committed and available
         * to the configuration or operational store.
         */
        // TODO: Implement the reconciliation (incremental stitching) algorithm
        log.debug("EVENT : NetworkElementedSynchronized : RECEIVED : {}, {}, {}", notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNetworkElementType());
    }

    @Override
    public void onNetworkElementSynchronizationFailure(NetworkElementSynchronizationFailure notification) {
        // no op
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
