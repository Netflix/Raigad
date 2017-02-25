/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.raigad.identity;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.AmazonInfo.MetaDataKey;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.shared.Application;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple class that implements HostSupplier. It provides a  Supplier<List<Host>>
 * using the {DiscoveryManager} which is the eureka client.
 *
 * Note that the class needs the eureka application name to discover all instances for that application.
 *
 */
@Singleton
public class EurekaHostsSupplier implements HostSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(EurekaHostsSupplier.class);

    private final DiscoveryClient discoveryClient;

    @Inject
    public EurekaHostsSupplier(DiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
    }

    @Override
    public Supplier<List<Host>> getSupplier(final String clusterName)
    {
        return new Supplier<List<Host>>() {

            @Override
            public List<Host> get() {

                if (discoveryClient == null) {
                    LOG.error("Discovery client cannot be null");
                    throw new RuntimeException("EurekaHostsSupplier needs a non-null DiscoveryClient");
                }

                LOG.debug("Raigad fetching instance list for app: " + clusterName);

                Application app = discoveryClient.getApplication(clusterName.toUpperCase());
                List<Host> hosts = new ArrayList<Host>();

                if (app == null) {
                    LOG.warn("Cluster '{}' not found in eureka", clusterName);
                    return hosts;
                }

                List<InstanceInfo> ins = app.getInstances();

                if (ins == null || ins.isEmpty()) {
                    LOG.warn("Cluster '{}' found in eureka but has no instances", clusterName);
                    return hosts;
                }

                hosts = Lists.newArrayList(Collections2.transform(
                        Collections2.filter(ins, new Predicate<InstanceInfo>() {
                            @Override
                            public boolean apply(InstanceInfo input) {
                                return input.getStatus() == InstanceInfo.InstanceStatus.UP;
                            }
                        }), new Function<InstanceInfo, Host>() {
                            @Override
                            public Host apply(InstanceInfo info) {
                                String[] parts = StringUtils.split(
                                        StringUtils.split(info.getHostName(), ".")[0], '-');

                                Host host = new Host(info.getHostName(), info.getPort())
                                        .addAlternateIpAddress(
                                                StringUtils.join(new String[] { parts[1], parts[2], parts[3],
                                                        parts[4] }, "."))
                                        .addAlternateIpAddress(info.getIPAddr())
                                        .setId(info.getId());

                                try {
                                    if (info.getDataCenterInfo() instanceof AmazonInfo) {
                                        AmazonInfo amazonInfo = (AmazonInfo)info.getDataCenterInfo();
                                        host.setRack(amazonInfo.get(MetaDataKey.availabilityZone));
                                    }
                                }
                                catch (Throwable t) {
                                    LOG.error("Error getting rack for host " + host.getName(), t);
                                }

                                return host;
                            }
                        }));

                LOG.debug("Raigad found hosts from eureka - num hosts: " + hosts.size());

                return hosts;
            }
        };
    }
}