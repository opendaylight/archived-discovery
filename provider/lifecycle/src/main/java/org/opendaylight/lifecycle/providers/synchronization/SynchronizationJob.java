/*
 * Copyright (c) 2014 Ciena Corporation and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.lifecycle.providers.synchronization;

import java.util.concurrent.Future;

import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementIdentified;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.NetworkElementTypes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.network.element.types.NetworkElementType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.network.element.types.NetworkElementTypeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.LifecycleSynchronizationService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.NetworkElementSynchronizationFailure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.NetworkElementSynchronizationFailureBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.NetworkElementSynchronized;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.NetworkElementSynchronizedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizeNetworkElementInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.SynchronizeNetworkElementOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.synchronize.network.element.output.Result;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.synchronize.network.element.output.result.Failure;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.synchronize.network.element.output.result.Pending;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.synchronization.rev140714.synchronize.network.element.output.result.Success;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around the invocation of an ODL Routed RPC to request that the information model of a network element be
 * synchronized from the physical network element. The wrapper synchronously invokes the RPC to synchronize the network
 * element and then publishes a notification based on the results of the synchronization process.
 *
 * @author David Bainbridge <dbainbri@ciena.com>
 * @since 2014-07-15
 */
public class SynchronizationJob implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SynchronizationJob.class);
    private final NetworkElementIdentified notification;
    private final RpcProviderRegistry rpcRegistry;
    private final NotificationProviderService notificationService;

    /**
     * Constructs new instance for a given network identified notification
     *
     * @param context
     *            used to locate ODL based RPCs
     * @param notificationService
     *            used to publish synchronization notifications or failures
     * @param notification
     *            event that triggered the synchronization and contains network element context
     * @see org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.identification.rev140714.NetworkElementIdentified
     */
    public SynchronizationJob(RpcProviderRegistry context, NotificationProviderService notificationService,
            NetworkElementIdentified notification) {
        this.notification = notification;
        this.rpcRegistry = context;
        this.notificationService = notificationService;
    }

    /**
     * Allows external access to the notification on which this synchronization is based.
     *
     * @return the notification the triggered the synchronization job
     */
    public final NetworkElementIdentified getNotification() {
        return notification;
    }

    /**
     * Invokes the ODL routed RPC to synchronize a network elements and then publishes a notification based on the
     * result.
     */
    @Override
    public void run() {
        try {
            log.debug("JOB : synchronization : RUN : {}, {}, {}", notification.getRequestId(),
                    notification.getNetworkElementIp(), notification.getNetworkElementType());
            // Invoke the routed RPC and wait for the result.
            LifecycleSynchronizationService service = rpcRegistry.getRpcService(LifecycleSynchronizationService.class);
            SynchronizeNetworkElementInputBuilder input = new SynchronizeNetworkElementInputBuilder();

            input.setRequestId(notification.getRequestId());
            input.setNetworkElementIp(notification.getNetworkElementIp());
            input.setNetworkElementType(notification.getNetworkElementType());
            input.setUsername(notification.getUsername());
            input.setPassword(notification.getPassword());

            InstanceIdentifier<NetworkElementType> id = InstanceIdentifier.create(NetworkElementTypes.class).child(
                    NetworkElementType.class, new NetworkElementTypeKey(notification.getNetworkElementType()));
            input.setNetworkElementTypeRef(id);

            log.debug("ROUTED_RPC : synchronizeNetworkElement : INVOKE : {}, {}, {}", input.getRequestId(),
                    input.getNetworkElementIp(), input.getNetworkElementType());

            Future<RpcResult<SynchronizeNetworkElementOutput>> future = service
                    .synchronizeNetworkElement(input.build());
            RpcResult<SynchronizeNetworkElementOutput> rpcresult = future.get();

            // Publish appropriate notification based on the result of the synchronization
            if (rpcresult.isSuccessful()) {
                final Result result = rpcresult.getResult().getResult();

                if (Success.class.isInstance(result)) {
                    /*
                     * Plugin complete the synchronization work successfully. Publish the notification for the plugin
                     */
                    final NetworkElementSynchronized event = new NetworkElementSynchronizedBuilder().setChanges(true)
                            .setRequestId(notification.getRequestId())
                            .setNetworkElementIp(notification.getNetworkElementIp())
                            .setNetworkElementType(notification.getNetworkElementType()).build();
                    log.debug("EVENT : NetworkElementedSynchronized : PUBLISH : {}, {}, {}", event.getRequestId(),
                            event.getNetworkElementIp(), event.getNetworkElementType());
                    notificationService.publish(event);
                } else if (Pending.class.isInstance(result)) {
                    /*
                     * The plugin will publish the appropriate notification when the device is synchronized.
                     */
                    log.debug("HANDOFF : synchronization : CHANGED : {}, {}, {}", notification.getRequestId(),
                            notification.getNetworkElementIp(), notification.getNetworkElementType());
                } else {
                    /*
                     * The plugin completed the work, but the work failed, so publish a failure notification on behalf
                     * of the plugin.
                     */
                    if (result == null) {
                        log.error(
                                "Synchronization of network element, request {}, identified by {} of type {} failed, unknown reason",
                                notification.getRequestId(), notification.getNetworkElementIp(),
                                notification.getNetworkElementType());
                        final NetworkElementSynchronizationFailure event = new NetworkElementSynchronizationFailureBuilder()
                                .setRequestId(notification.getRequestId())
                                .setNetworkElementIp(notification.getNetworkElementIp())
                                .setNetworkElementType(notification.getNetworkElementType()).setCause("unknown")
                                .build();
                        log.debug("EVENT : NetworkElementSynchronizationFailure : PUBLISH : {}, {}, {}",
                                event.getRequestId(), event.getNetworkElementIp(), event.getNetworkElementType());
                        notificationService.publish(event);
                    } else {
                        final Failure failure = Failure.class.cast(result);
                        log.error(
                                "Synchronization of network element identified by {} of type {} failed, unknown reason",
                                notification.getNetworkElementIp(), notification.getNetworkElementType());
                        final NetworkElementSynchronizationFailure event = new NetworkElementSynchronizationFailureBuilder()
                                .setNetworkElementIp(notification.getNetworkElementIp())
                                .setNetworkElementType(notification.getNetworkElementType())
                                .setCause(failure.getCause()).build();
                        log.debug("EVENT : NetworkElementSynchronizationFailure : PUBLISH : {}, {}, {}",
                                event.getRequestId(), event.getNetworkElementIp(), event.getNetworkElementType());
                        notificationService.publish(event);
                    }
                }
            } else {
                /*
                 * There was something wrong with the RPC handling. This is a failure, so publish the notification
                 */
                log.error(
                        "Synchronization of network element, request {}, identified by {} of type {} failed, reported errors {}",
                        notification.getRequestId(), notification.getNetworkElementIp(),
                        notification.getNetworkElementType(), rpcresult.getErrors());
                NetworkElementSynchronizationFailure event = new NetworkElementSynchronizationFailureBuilder()
                        .setRequestId(notification.getRequestId())
                        .setNetworkElementIp(notification.getNetworkElementIp())
                        .setNetworkElementType(notification.getNetworkElementType())
                        .setCause(rpcresult.getErrors().toString()).build();
                log.debug("EVENT : NetworkElementSynchronizationFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                        event.getNetworkElementIp(), event.getNetworkElementType());
                notificationService.publish(event);
            }
        } catch (Exception e) {
            /*
             * If error occurred while attempting to synchronize then attempt to publish this as a synchronization
             * failure. If that also fails, then there is not much more we can do so log the error.
             */
            log.error(
                    "Unable to synchronize network element, request {}, identified by {} of type {}, reported cause, {} : {}",
                    notification.getRequestId(), notification.getNetworkElementIp(),
                    notification.getNetworkElementType(), e.getClass().getName(), e.getMessage());
            try {
                NetworkElementSynchronizationFailure event = new NetworkElementSynchronizationFailureBuilder()
                        .setRequestId(notification.getRequestId())
                        .setNetworkElementIp(notification.getNetworkElementIp())
                        .setNetworkElementType(notification.getNetworkElementType())
                        .setCause(String.format("%s : %s", e.getClass().getName(), e.getMessage())).build();
                log.debug("EVENT : NetworkElementSynchronizationFailure : PUBLISH : {}, {}, {}", event.getRequestId(),
                        event.getNetworkElementIp(), event.getNetworkElementType());
                notificationService.publish(event);
            } catch (Exception e1) {
                log.error(
                        "Unable to publish synchronization fail of network element, request {}, identified by {} of type {}, reported cause, {} : {}",
                        notification.getRequestId(), notification.getNetworkElementIp(),
                        notification.getNetworkElementType(), e.getClass().getName(), e.getMessage());
            }
        }
    }
}
