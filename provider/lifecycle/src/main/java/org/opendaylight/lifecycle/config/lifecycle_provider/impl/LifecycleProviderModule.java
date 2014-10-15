/*
 * Copyright (c) 2014 Ciena Corporation and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.lifecycle.config.lifecycle_provider.impl;

import org.opendaylight.controller.config.api.DependencyResolver;
import org.opendaylight.controller.config.api.ModuleIdentifier;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RpcRegistration;
import org.opendaylight.lifecycle.providers.identification.IdentificationProvider;
import org.opendaylight.lifecycle.providers.reconciliation.ReconciliationProvider;
import org.opendaylight.lifecycle.providers.state.StateProvider;
import org.opendaylight.lifecycle.providers.synchronization.SynchronizationProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.LifecycleService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.LifecycleStates;
import org.opendaylight.yang.gen.v1.urn.opendaylight.lifecycle.rev140714.LifecycleStatesBuilder;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier.InstanceIdentifierBuilder;
import org.opendaylight.yangtools.yang.binding.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LifecycleProviderModule extends AbstractLifecycleProviderModule {
    private static Logger log = LoggerFactory.getLogger(LifecycleProviderModule.class);

    public LifecycleProviderModule(ModuleIdentifier identifier, DependencyResolver dependencyResolver) {
        super(identifier, dependencyResolver);
    }

    public LifecycleProviderModule(ModuleIdentifier identifier, DependencyResolver dependencyResolver,
            LifecycleProviderModule oldModule, AutoCloseable oldInstance) {
        super(identifier, dependencyResolver, oldModule, oldInstance);
    }

    @Override
    public AutoCloseable createInstance() {
        log.debug("CREATE : {}", Integer.toHexString(this.hashCode()));

        /*
         * Create the base container for state information about network element lifecycle
         */
        final InstanceIdentifierBuilder<LifecycleStates> container = InstanceIdentifier.builder(LifecycleStates.class);
        LifecycleStatesBuilder lifecycleStates = new LifecycleStatesBuilder();
        final ReadWriteTransaction wo = getDataBrokerDependency().newReadWriteTransaction();
        wo.put(LogicalDatastoreType.OPERATIONAL, container.build(), lifecycleStates.build());
        wo.submit();

        final StateProvider stateProvider = new StateProvider(getNotificationServiceDependency(),
                getDataBrokerDependency());
        final ListenerRegistration<NotificationListener> stateNotificationReg = getNotificationServiceDependency()
                .registerNotificationListener(stateProvider);

        final IdentificationProvider identificationProvider = new IdentificationProvider(
                getNotificationServiceDependency(), getDataBrokerDependency());
        final ListenerRegistration<NotificationListener> identificationNotificationReg = getNotificationServiceDependency()
                .registerNotificationListener(identificationProvider);
        final RpcRegistration<LifecycleService> identificationRpcReg = getRpcRegistryDependency().addRpcImplementation(
                LifecycleService.class, identificationProvider);

        final SynchronizationProvider synchronizationProvider = new SynchronizationProvider(
                getNotificationServiceDependency(), getRpcRegistryDependency(), getSynchronizationThreadPoolSize());
        final ListenerRegistration<NotificationListener> synchronizationNotificationReg = getNotificationServiceDependency()
                .registerNotificationListener(synchronizationProvider);

        final ReconciliationProvider reconciliationProvider = new ReconciliationProvider();

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
                        log.error("Unable to close lifecycle state provider notification registration");
                    }
                }

                if (identificationNotificationReg != null) {
                    try {
                        identificationNotificationReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close lifecycle identification provider notification registration");
                    }
                }

                if (identificationRpcReg != null) {
                    try {
                        identificationRpcReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close lifecycle identification provider RPC registration");
                    }
                }

                if (synchronizationNotificationReg != null) {
                    try {
                        synchronizationNotificationReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close lifecycle synchronization provider notification registration");
                    }
                }

                stateProvider.close();
                identificationProvider.close();
                synchronizationProvider.close();
                reconciliationProvider.close();

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
