/*
 * Copyright (c) 2014 Ciena Corporation and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.discovery;

import java.util.HashMap;
import java.util.Map;

import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.NetworkElementIdentified;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.identification.rev140714.UnableToIdentifyNetworkElement;
import org.opendaylight.yang.gen.v1.urn.opendaylight.discovery.rev140714.NewNetworkElement;

import com.google.common.collect.ComputationException;

/**
 * @author dbainbri
 *
 */
public class NEID {
    public static final String IPV4_ADDRESS = "IPV4_ADDRESS";
    public static final Map<String, NEID> cache = new HashMap<String, NEID>();

    private final String value;

    private NEID(String value) {
        this.value = value;
    }

    public final String getValue() {
        return value;
    }

    private static final NEID getOrUpdate(String key) {
        NEID id = cache.get(key);
        if (id == null) {
            synchronized (cache) {
                if ((id = cache.get(key)) == null) {
                    id = new NEID(key);
                    cache.put(key, id);
                }
            }
        }
        return id;
    }

    public static final NEID fromContraints(Map<String, String> contraints) {
        if (contraints.containsKey(IPV4_ADDRESS)) {
            return getOrUpdate(contraints.get(IPV4_ADDRESS));
        }

        throw new ComputationException(new RuntimeException("Can currently only use IPv4 addresses"));
    }

    /**
     * @param notification
     * @return
     */
    public static NEID fromNetworkElement(NewNetworkElement notification) {
        Map<String, String> contraints = new HashMap<String, String>();
        contraints.put(IPV4_ADDRESS, notification.getNetworkElementIp());
        return fromContraints(contraints);
    }

    /**
     * @param notification
     * @return
     */
    public static NEID fromNetworkElement(NetworkElementIdentified notification) {
        Map<String, String> contraints = new HashMap<String, String>();
        contraints.put(IPV4_ADDRESS, notification.getNetworkElementIp());
        return fromContraints(contraints);
    }

    /**
     * @param notification
     * @return
     */
    public static NEID fromNetworkElement(UnableToIdentifyNetworkElement notification) {
        Map<String, String> contraints = new HashMap<String, String>();
        contraints.put(IPV4_ADDRESS, notification.getNetworkElementIp());
        return fromContraints(contraints);
    }

    /**
     * @param networkElementIp
     * @param networkElementType
     * @return
     */
    public static NEID fromParts(String networkElementIp, String networkElementType) {
        Map<String, String> contraints = new HashMap<String, String>();
        contraints.put(IPV4_ADDRESS, networkElementIp);
        return fromContraints(contraints);
    }

}
