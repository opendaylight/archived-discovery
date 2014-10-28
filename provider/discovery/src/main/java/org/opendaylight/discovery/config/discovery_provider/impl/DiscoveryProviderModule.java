/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.config.discovery_provider.impl;

import org.opendaylight.controller.config.api.DependencyResolver;
import org.opendaylight.controller.config.api.ModuleIdentifier;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RpcRegistration;
import org.opendaylight.discovery.providers.communication.CommunicationProvider;
import org.opendaylight.discovery.providers.deletion.DeletionProvider;
import org.opendaylight.discovery.providers.identification.IdentificationProvider;
import org.opendaylight.discovery.providers.reconciliation.ReconciliationProvider;
import org.opendaylight.discovery.providers.state.StateProvider;
import org.opendaylight.discovery.providers.synchronization.SynchronizationProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.DiscoveryStatesBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.IpToNodeIds;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.IpToNodeIdsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NetworkElementTypes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NetworkElementTypesBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NodeIdToStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NodeIdToStatesBuilder;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.opendaylight.yangtools.yang.binding.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoveryProviderModule extends AbstractDiscoveryProviderModule {
    private static Logger log = LoggerFactory.getLogger(DiscoveryProviderModule.class);

    public DiscoveryProviderModule(ModuleIdentifier identifier, DependencyResolver dependencyResolver) {
        super(identifier, dependencyResolver);
    }

    public DiscoveryProviderModule(ModuleIdentifier identifier, DependencyResolver dependencyResolver,
            DiscoveryProviderModule oldModule, AutoCloseable oldInstance) {
        super(identifier, dependencyResolver, oldModule, oldInstance);
    }

    @Override
    public AutoCloseable createInstance() {
        log.debug("CREATE : {}", Integer.toHexString(this.hashCode()));

        /*
         * Create the base container for state information about network element discovery
         */
        final InstanceIdentifierBuilder<DiscoveryStates> stateContainer = InstanceIdentifier
                .builder(DiscoveryStates.class);
        DiscoveryStatesBuilder discoveryStates = new DiscoveryStatesBuilder();
        ReadWriteTransaction wo = getDataBrokerDependency().newReadWriteTransaction();
        wo.merge(LogicalDatastoreType.OPERATIONAL, stateContainer.build(), discoveryStates.build());
        wo.submit();

        final InstanceIdentifierBuilder<NetworkElementTypes> neTypeContainer = InstanceIdentifier
                .builder(NetworkElementTypes.class);
        NetworkElementTypesBuilder neTypes = new NetworkElementTypesBuilder();
        wo = getDataBrokerDependency().newReadWriteTransaction();
        wo.merge(LogicalDatastoreType.OPERATIONAL, neTypeContainer.build(), neTypes.build());
        wo.submit();

        final InstanceIdentifierBuilder<NodeIdToStates> nodeIdToGuidContainer = InstanceIdentifier
                .builder(NodeIdToStates.class);
        NodeIdToStatesBuilder nodeIdToGuid = new NodeIdToStatesBuilder();
        wo = getDataBrokerDependency().newReadWriteTransaction();
        wo.merge(LogicalDatastoreType.OPERATIONAL, nodeIdToGuidContainer.build(), nodeIdToGuid.build());
        wo.submit();

        final InstanceIdentifierBuilder<IpToNodeIds> ipToGuidContainer = InstanceIdentifier.builder(IpToNodeIds.class);
        IpToNodeIdsBuilder ipToGuid = new IpToNodeIdsBuilder();
        wo = getDataBrokerDependency().newReadWriteTransaction();
        wo.merge(LogicalDatastoreType.OPERATIONAL, ipToGuidContainer.build(), ipToGuid.build());
        wo.submit();

        final IdentificationProvider identificationProvider = new IdentificationProvider(
                getNotificationServiceDependency(), getDataBrokerDependency());
        final ListenerRegistration<NotificationListener> identificationNotificationReg = getNotificationServiceDependency()
                .registerNotificationListener(identificationProvider);
        final RpcRegistration<DiscoveryService> identificationRpcReg = getRpcRegistryDependency().addRpcImplementation(
                DiscoveryService.class, identificationProvider);

        final SynchronizationProvider synchronizationProvider = new SynchronizationProvider(
                getNotificationServiceDependency(), getRpcRegistryDependency(), getSynchronizationThreadPoolSize(),
                getDataBrokerDependency());
        final ListenerRegistration<NotificationListener> synchronizationNotificationReg = getNotificationServiceDependency()
                .registerNotificationListener(synchronizationProvider);

        final StateProvider stateProvider = new StateProvider(getNotificationServiceDependency(),
                getDataBrokerDependency());
        final ListenerRegistration<NotificationListener> stateNotificationReg = getNotificationServiceDependency()
                .registerNotificationListener(stateProvider);

        final ReconciliationProvider reconciliationProvider = new ReconciliationProvider();

        final CommunicationProvider communicationProvider = new CommunicationProvider(
                getNotificationServiceDependency());
        final ListenerRegistration<NotificationListener> communicationNotificationReg = getNotificationServiceDependency()
                .registerNotificationListener(communicationProvider);

        final DeletionProvider deletionProvider = new DeletionProvider(
                getNotificationServiceDependency(), getRpcRegistryDependency(), getSynchronizationThreadPoolSize(),
                getDataBrokerDependency());
        final ListenerRegistration<NotificationListener> deletionNotificationReg = getNotificationServiceDependency()
                .registerNotificationListener(deletionProvider);

        /*
         * Wrap AutoCloseable and close registrations to md-sal at close()
         */
        final class CloseResources implements AutoCloseable {

            @Override
            public void close() throws Exception {
                if (stateNotificationReg != null) {
                    try {
                        stateNotificationReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close discovery state provider notification registration");
                    }
                }

                if (identificationNotificationReg != null) {
                    try {
                        identificationNotificationReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close discovery identification provider notification registration");
                    }
                }

                if (identificationRpcReg != null) {
                    try {
                        identificationRpcReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close discovery identification provider RPC registration");
                    }
                }

                if (synchronizationNotificationReg != null) {
                    try {
                        synchronizationNotificationReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close discovery synchronization provider notification registration");
                    }
                }

                if (communicationNotificationReg != null) {
                    try {
                        communicationNotificationReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close discovery communication provider notification registration");
                    }
                }

                if (deletionNotificationReg != null) {
                    try {deletionNotificationReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close discovery deletion provider notification registration");
                    }
                }
                stateProvider.close();
                identificationProvider.close();
                synchronizationProvider.close();
                communicationProvider.close();
                reconciliationProvider.close();
                deletionProvider.close();


                if (log.isDebugEnabled()) {
                    log.debug("TEAR-DOWN : {}", Integer.toHexString(this.hashCode()));
                }
            }
        }

        AutoCloseable ret = new CloseResources();
        log.debug("INITIALIZED : {}", Integer.toHexString(this.hashCode()));
        return ret;
    }
}
