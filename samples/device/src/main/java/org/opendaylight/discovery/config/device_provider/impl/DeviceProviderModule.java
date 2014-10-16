/*
 * Copyright (c) 2014 Ciena Corporation and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery.config.device_provider.impl;

import org.opendaylight.controller.config.api.DependencyResolver;
import org.opendaylight.controller.config.api.ModuleIdentifier;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.RoutedRpcRegistration;
import org.opendaylight.discovery.samples.device.DeviceProvider;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NetworkElementTypeContext;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NetworkElementTypes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.network.element.types.NetworkElementType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.network.element.types.NetworkElementTypeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.synchronization.rev140714.DiscoverySynchronizationService;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.binding.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceProviderModule extends AbstractDeviceProviderModule {
    private static Logger log = LoggerFactory.getLogger(DeviceProviderModule.class);
    private ListenerRegistration<NotificationListener> notificationReg = null;
    private RoutedRpcRegistration<DiscoverySynchronizationService> rpcReg = null;
    private ListenerRegistration<DataChangeListener> dataReg = null;

    public DeviceProviderModule(ModuleIdentifier identifier, DependencyResolver dependencyResolver) {
        super(identifier, dependencyResolver);
    }

    public DeviceProviderModule(ModuleIdentifier identifier, DependencyResolver dependencyResolver,
            DeviceProviderModule oldModule, AutoCloseable oldInstance) {
        super(identifier, dependencyResolver, oldModule, oldInstance);
    }

    @Override
    public AutoCloseable createInstance() {
        log.debug("CREATE : {}", Integer.toHexString(this.hashCode()));
        final DeviceProvider provider = new DeviceProvider(getNotificationServiceDependency());

        notificationReg = getNotificationServiceDependency().registerNotificationListener(provider);
        // dataChangeRegistration = this.dataBroker.registerDataChangeListener(LogicalDatastoreType.CONFIGURATION, path,
        // this, DataChangeScope.ONE);

        final InstanceIdentifier<NetworkElementType> DEVICE_TYPE_ID = InstanceIdentifier
                .builder(NetworkElementTypes.class)
                .child(NetworkElementType.class, new NetworkElementTypeKey(DeviceProvider.DEVICE_TYPE_TAG)).build();
        rpcReg = getRpcRegistryDependency().addRoutedRpcImplementation(DiscoverySynchronizationService.class, provider);

        // InstanceIdentifier<NetworkElementType> DEVICE_TYPE_ID = InstanceIdentifier.create(NetworkElementTypes.class)
        // .child(NetworkElementType.class, new NetworkElementTypeKey("SAMPLE"));

        // final InstanceIdentifier<NetworkElementType> DEVICE_TYPE_ID = InstanceIdentifier.create(
        // NetworkElementTypes.class).child(NetworkElementType.class,
        // new NetworkElementTypeKey(DeviceProvider.DEVICE_TYPE_TAG));
        rpcReg.registerPath(NetworkElementTypeContext.class, DEVICE_TYPE_ID);

        /*
         * Wrap AutoCloseable and close registrations to md-sal at close()
         */
        final class CloseResources implements AutoCloseable {

            @Override
            public void close() throws Exception {
                if (notificationReg != null) {
                    try {
                        notificationReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close notification registration");
                    } finally {
                        notificationReg = null;
                    }
                }

                if (rpcReg != null) {
                    try {
                        rpcReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close RPC registration");
                    } finally {
                        rpcReg = null;
                    }
                }

                if (dataReg != null) {
                    try {
                        dataReg.close();
                    } catch (Exception e) {
                        log.error("Unable to close data broker registration");
                    } finally {
                        dataReg = null;
                    }
                }

                provider.close();
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
