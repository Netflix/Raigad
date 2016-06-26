/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.raigad.discovery;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

public class RaigadDiscoveryUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {
    private final TransportService transportService;
    private final Version version;
    private final Settings settings;

    @Inject
    public RaigadDiscoveryUnicastHostsProvider(Settings settings, TransportService transportService, Version version) {
        super(settings);
        this.transportService = transportService;
        this.version = version;
        this.settings = settings;
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();

        try {
            //Extract tribe ID from name field of settings and query accordingly
            String strNodes;

            if (isCurrentNodeTribe(settings)) {
                String nodeName = settings.get("name");
                //TODO: Check for null node name
                String tribeId = nodeName.substring(nodeName.indexOf("/") + 1);
                logger.debug("Tribe node name: {}, tribe ID: {}", nodeName, tribeId);
                strNodes = DataFetcher.fetchData(RaigadDiscovery.GET_NODES_TRIBE_URL_PREFIX + tribeId, logger);
            }
            else {
                strNodes = DataFetcher.fetchData(RaigadDiscovery.GET_NODES_ISLAND_URL, logger);
            }

            List<RaigadInstance> instances = ElasticsearchUtil.getRaigadInstancesFromJsonString(strNodes, logger);

            for (RaigadInstance instance : instances) {
                try {
                    TransportAddress[] addresses =
                            transportService.addressesFromString(instance.getHostIP(), UnicastZenPing.LIMIT_FOREIGN_PORTS_COUNT);

                    // We only limit to 1 port, makes no sense to ping 100 ports

                    for (int i = 0; (i < addresses.length && i < UnicastZenPing.LIMIT_FOREIGN_PORTS_COUNT); i ++) {
                        logger.debug("Adding instance {} (address {}, transport address {})",
                                instance.getId(), instance.getHostIP(), addresses[i]);
                        discoveryNodes.add(new DiscoveryNode(instance.getId(), addresses[i], version.minimumCompatibilityVersion()));
                    }
                }
                catch (Exception e) {
                    logger.warn("Failed to add instance {} (address {})", e, instance.getId(), instance.getHostIP());
                }
            }
        }
        catch (Exception e) {
            logger.error("Exception while trying to build dynamic discovery nodes", e);
            throw new RuntimeException(e);
        }

        logger.info("Using the following dynamic discovery nodes: {}", discoveryNodes);

        return discoveryNodes;
    }

    private boolean isCurrentNodeTribe(Settings settings) {
        boolean currentNodeTribe = false;

        if (settings != null && !StringUtils.isEmpty(settings.get("name"))) {
            String tribeName = settings.get("name");
            if (tribeName.contains("/t")) {
                currentNodeTribe = true;
            }
        }

        return currentNodeTribe;
    }
}
