/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.deletion;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DeleteNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DiscoveryDeletionListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeleted;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeletionFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NodeIdToStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToState;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.node.id.to.states.NodeIdToStateKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.CheckedFuture;

/**
 * OSGi/ODL Activator that registers handlers for notifications pertaining to
 * network elements being identified. If a network element is identified then
 * the handler queues up a request to synchronize the network element. If a
 * network element fails to be identified then an error message is logged.
 *
 * @author Gunjan Patel <gupatel@ciena.com>
 * @since 2014-10-25
 */
public class DeletionProvider implements DiscoveryDeletionListener,
AutoCloseable {
    private static final Logger log = LoggerFactory
            .getLogger(DeletionProvider.class);
    private final ExecutorService executor;
    private final NotificationProviderService notificationProviderService;
    private final RpcProviderRegistry rpcRegistry;
    private final DataBroker dataBroker;

    public DeletionProvider(
            NotificationProviderService notificationProviderService,
            RpcProviderRegistry rpcRegistry, int threadPoolSize,
            DataBroker dataBroker) {
        this.notificationProviderService = notificationProviderService;
        this.rpcRegistry = rpcRegistry;
        this.dataBroker = dataBroker;
        log.debug("CREATE : {} : SIZE = {}",
                Integer.toHexString(this.hashCode()), threadPoolSize);
        executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public void close() throws Exception {
        // TODO Auto-generated method stub

    }

    /*
     * start DeletionJob as soon as DeleteNE notification is raised
     */
    @Override
    public void onDeleteNetworkElement(DeleteNetworkElement notification) {

        log.debug(
                "EVENT : DeleteNetworkElement Notification : RECEIVED : {}, {}, {}",
                notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNodeId());
        executor.submit(new DeletionJob(rpcRegistry,
                notificationProviderService, notification));
        log.debug("JOB : deletion : SUMMITED : {}, {}, {}",
                notification.getRequestId(),
                notification.getNetworkElementIp(), notification.getNodeId());

    }

    @Override
    public void onNetworkElementDeleted(NetworkElementDeleted notification) {

        final InstanceIdentifierBuilder<NodeIdToState> id = InstanceIdentifier
                .builder(NodeIdToStates.class).child(NodeIdToState.class,
                        new NodeIdToStateKey(notification.getNodeId()));

        final ReadWriteTransaction wo = dataBroker.newReadWriteTransaction();
        wo.delete(LogicalDatastoreType.OPERATIONAL, id.build());

        CheckedFuture<Void, TransactionCommitFailedException> sync = wo
                .submit();
        try {
            sync.checkedGet();
        } catch (TransactionCommitFailedException e) {
            log.error(
                    String.format(
                            "Error while attempting to delete NE from node-id to state table for request: {}, node-id: {}",
                            notification.getRequestId(), notification.getNodeId(), e));
        }
        log.debug("STORE : DELETE : /node-id-to-states/node-id-to-state/{},  ReqID: {} ", notification.getNodeId(), notification.getRequestId());
    }

    @Override
    public void onNetworkElementDeletionFailure(
            NetworkElementDeletionFailure notification) {
        // TODO Auto-generated method stub

    }

}