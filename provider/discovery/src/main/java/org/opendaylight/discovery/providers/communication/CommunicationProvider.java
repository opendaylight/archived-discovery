/*
 * Copyright (c) 2014 Ciena Corporation. All rights reserved. This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License v1.0 which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.communication;

import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.DiscoveryCommunicationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.NetworkElementCommunicationDown;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.NetworkElementCommunicationRestored;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRemovedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to network elements being identified. If a
 * network element is identified then the handler queues up a request to synchronize the network element. If a network
 * element fails to be identified then an error message is logged.
 *
 * @author Rahul Sharma <rahushar@ciena.com>
 * @since 2014-10-20
 */
public class CommunicationProvider implements DiscoveryCommunicationListener, AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(CommunicationProvider.class);
    private final NotificationProviderService notificationService;

    public CommunicationProvider(NotificationProviderService notificationProviderService) {
        this.notificationService = notificationProviderService;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.DiscoveryCommunicationListener
     * #onNetworkElementCommunicationDown
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.NetworkElementCommunicationDown)
     */
    @Override
    public void onNetworkElementCommunicationDown(NetworkElementCommunicationDown notification) {
        log.warn("Communication with Node = {} has gone down... It will now be deleted from Controller's management.",
                notification.getNodeId());

        String nodeId = notification.getNodeId();
        InstanceIdentifier<Node> identifier = identifierFromNeId(nodeId);

        if (identifier != null) {
            NodeRemovedBuilder builder = new NodeRemovedBuilder();
            builder.setNodeRef(new NodeRef(identifier));

            notificationService.publish(builder.build());
            log.warn("Node = {} deleted from Controller's management.", notification.getNodeId());
        } else {
            log.error("'null' nodeId is obtained in the notification");
        }

    }

    InstanceIdentifier<Node> identifierFromNeId(String neName) {
        if (neName != null) {
            NodeKey nodeKey = new NodeKey(new NodeId(neName));
            InstanceIdentifierBuilder<Node> builder = InstanceIdentifier.builder(Nodes.class)
                    .child(Node.class, nodeKey);
            return builder.toInstance();
        } else {
            return null;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020.DiscoveryCommunicationListener
     * #onNetworkElementCommunicationRestored
     * (org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.communication.rev141020
     * .NetworkElementCommunicationRestored)
     */
    @Override
    public void onNetworkElementCommunicationRestored(NetworkElementCommunicationRestored notification) {
        /*
         * No op - This notification is handled by the synchronization manager as on comms restore the network element
         * should be re-synced. If additional behavior is needed on a comms restore that is unrelated to synchronization
         * then this method should be implemented.
         *
         * We need to keep the empyt method because ODL (MD-SAL really) on deals with interfaces not individual
         * notifications. Silly MD-SAL.
         */
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
