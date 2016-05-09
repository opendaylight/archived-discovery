/*
 * Copyright (c) 2014 Ciena Corporation and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.providers.deletion;

import java.util.concurrent.Future;

import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DestroyNetworkElementInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DestroyNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.DiscoveryDeletionService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.FinalizeNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeleted;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeletedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeletionFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.NetworkElementDeletionFailureBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.destroy.network.element.output.Result;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.destroy.network.element.output.result.Failure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.destroy.network.element.output.result.Pending;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.deletion.rev140714.destroy.network.element.output.result.Success;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NetworkElementTypes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.network.element.types.NetworkElementType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.network.element.types.NetworkElementTypeKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable for Routed RPC that processes the delete-network-element process and
 * returns appropriate notification upon success or failure
 *
 * @author Gunjan Patel <gupatel@ciena.com>
 * @since 2014-10-25
 */

public class DeletionJob implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DeletionJob.class);
    private final FinalizeNetworkElement notification;
    private final RpcProviderRegistry rpcRegistry;
    private final NotificationProviderService notificationService;

    /**
     * Constructs new instance for a given network identified notification
     *
     * @param context
     *            used to locate ODL based RPCs
     * @param notificationService
     *            used to publish deletion notifications or failures
     * @param notification
     *            event that triggered the deletion and contains network element
     *            context
     */

    public DeletionJob(RpcProviderRegistry context, NotificationProviderService notificationService,
            FinalizeNetworkElement notification) {
        this.notification = notification;
        this.rpcRegistry = context;
        this.notificationService = notificationService;
    }

    /**
     *
     * @return the notification the triggered the deletion job
     */
    public final FinalizeNetworkElement getNotification() {
        return this.notification;
    }

    /**
     * Invokes the ODL routed RPC to destroy/delete a network element and then
     * publishes a notification based on the result.
     */
    @Override
    public void run() {
        try {
            log.debug("JOB : Delete: destroy : RUN : {}, {}, {}", this.notification.getRequestId(),
                    this.notification.getNetworkElementIp(), this.notification.getNodeId());
            // Invoke the routed RPC and wait for the result.
            DiscoveryDeletionService service = this.rpcRegistry.getRpcService(DiscoveryDeletionService.class); // returns
            // the proxy
            DestroyNetworkElementInputBuilder input = new DestroyNetworkElementInputBuilder(); // rpc
            // input

            input.setRequestId(this.notification.getRequestId());
            input.setNetworkElementIp(this.notification.getNetworkElementIp());
            input.setNetworkElementType(this.notification.getNetworkElementType());
            input.setUsername(this.notification.getUsername());
            input.setPassword(this.notification.getPassword());
            input.setNodeId(this.notification.getNodeId());

            InstanceIdentifier<NetworkElementType> id = InstanceIdentifier.builder(NetworkElementTypes.class)
                    .child(NetworkElementType.class, new NetworkElementTypeKey(notification.getNetworkElementType()))
                    .build();
            input.setNetworkElementTypeRef(id);

            log.debug("ROUTED_RPC : Delete: destroyNetworkElement : INVOKE : {}, {}, {}", input.getRequestId(),
                    input.getNetworkElementIp(), input.getNodeId());

            Future<RpcResult<DestroyNetworkElementOutput>> future = service.destroyNetworkElement(input.build());

            RpcResult<DestroyNetworkElementOutput> rpcresult = future.get();
            // Publish appropriate notification based on the result of the
            // deletion
            if (rpcresult.isSuccessful()) {
                final Result result = rpcresult.getResult().getResult();
                if (Success.class.isInstance(result)) {
                    /*
                     * Plugin complete the deletion work successfully. Publish
                     * the notification for the plugin
                     */
                    final NetworkElementDeleted event = new NetworkElementDeletedBuilder().setChanges(true)
                            .setRequestId(this.notification.getRequestId())
                            .setNetworkElementIp(this.notification.getNetworkElementIp())
                            .setNodeId(this.notification.getNodeId())
                            .setNetworkElementType(this.notification.getNetworkElementType()).build();
                    log.debug("EVENT : Delete: NetworkElementDeleted : PUBLISH : {}, {}, {}", event.getRequestId(),
                            event.getNetworkElementIp(), event.getNetworkElementType());
                    this.notificationService.publish(event);
                } else if (Pending.class.isInstance(result)) {
                    /*
                     * The plugin will publish the appropriate notification when
                     * the device is deleted.
                     */
                    log.debug("HANDOFF : deletion : CHANGED : {}, {}, {}", this.notification.getRequestId(),
                            this.notification.getNetworkElementIp(), this.notification.getNetworkElementType());
                } else {
                    /*
                     * The plugin completed the work, but the work failed, so
                     * publish a failure notification on behalf of the plugin.
                     */
                    if (result == null) {
                        log.error(
                                "Deletion of network element, Request {}, identified by {} of type {} failed, unknown reason",
                                this.notification.getRequestId(), this.notification.getNetworkElementIp(),
                                this.notification.getNetworkElementType());
                        final NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                                .setRequestId(this.notification.getRequestId())
                                .setNetworkElementIp(this.notification.getNetworkElementIp())
                                .setNodeId(this.notification.getNodeId())
                                .setNetworkElementType(this.notification.getNetworkElementType()).setCause("unknown")
                                .build();
                        log.debug("EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                                event.getNetworkElementIp(), event.getNetworkElementType());
                        this.notificationService.publish(event);
                    } else {
                        final Failure failure = Failure.class.cast(result);
                        log.error("Deletion of network element identified by {} of type {} failed, unknown reason",
                                this.notification.getNetworkElementIp(), this.notification.getNetworkElementType());
                        final NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                                .setNetworkElementIp(this.notification.getNetworkElementIp())
                                .setNetworkElementType(this.notification.getNetworkElementType())
                                .setNodeId(this.notification.getNodeId()).setCause(failure.getCause()).build();
                        log.debug("EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                                event.getNetworkElementIp(), event.getNetworkElementType());
                        this.notificationService.publish(event);
                    }
                }
            } else {
                /*
                 * There was something wrong with the RPC handling. This is a
                 * failure, so publish the notification
                 */
                log.error(
                        "Deletion of network element, Request {}, identified by {} of type {} failed, reported errors {}",
                        this.notification.getRequestId(), this.notification.getNetworkElementIp(),
                        this.notification.getNetworkElementType(), rpcresult.getErrors());
                NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                        .setRequestId(this.notification.getRequestId())
                        .setNetworkElementIp(this.notification.getNetworkElementIp())
                        .setNetworkElementType(this.notification.getNetworkElementType())
                        .setNodeId(this.notification.getNodeId()).setCause(rpcresult.getErrors().toString()).build();
                log.debug("EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                        event.getNetworkElementIp(), event.getNetworkElementType());
                this.notificationService.publish(event);
            }
        } catch (Exception e) {
            /*
             * If error occurred while attempting to delete then attempt to
             * publish this as a deletion failure. If that also fails, then
             * there is not much more we can do so log the error.
             */
            log.error(
                    "Unable to delete network element, Request {}, identified by {} of type {}, reported cause, {} : {}",
                    this.notification.getRequestId(), this.notification.getNetworkElementIp(),
                    this.notification.getNetworkElementType(), e.getClass().getName(), e.getMessage());
            try {
                NetworkElementDeletionFailure event = new NetworkElementDeletionFailureBuilder()
                        .setRequestId(this.notification.getRequestId())
                        .setNetworkElementIp(this.notification.getNetworkElementIp())
                        .setNetworkElementType(this.notification.getNetworkElementType())
                        .setNodeId(this.notification.getNodeId())
                        .setCause(String.format("%s : %s", e.getClass().getName(), e.getMessage())).build();
                log.debug("EVENT : NetworkElementDeletionFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                        event.getNetworkElementIp(), event.getNetworkElementType());
                this.notificationService.publish(event);
            } catch (Exception e1) {
                log.error(
                        "Unable to publish deletion fail of network element, Request {}, identified by {} of type {}, reported cause, {} : {}",
                        this.notification.getRequestId(), this.notification.getNetworkElementIp(),
                        this.notification.getNetworkElementType(), e.getClass().getName(), e.getMessage());
            }
        }
    }

}
