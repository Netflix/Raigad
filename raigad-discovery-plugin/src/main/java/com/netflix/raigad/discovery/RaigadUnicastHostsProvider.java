/**
 * Copyright 2017 Netflix, Inc.
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

import com.netflix.raigad.discovery.utils.DataFetcher;
import com.netflix.raigad.discovery.utils.ElasticsearchUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

public class RaigadUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    private static final String GET_NODES_ISLAND_URL = "http://127.0.0.1:8080/Raigad/REST/v1/esconfig/get_nodes";
    private static final String GET_NODES_TRIBE_URL_PREFIX = "http://127.0.0.1:8080/Raigad/REST/v1/esconfig/get_tribe_nodes/";

    private final TransportService transportService;

    RaigadUnicastHostsProvider(Settings settings, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {

        final List<DiscoveryNode> discoveryNodes = new ArrayList<>();

        try {
            //Extract tribe ID from name field of settings and query accordingly
            String discoveryNodesJsonString;

            if (isTribeNode()) {
                String nodeName = settings.get("name");
                String tribeId = nodeName.substring(nodeName.indexOf("/") + 1);
                logger.debug("[raigad-discovery] Tribe node name [{}], ID [{}]", nodeName, tribeId);
                discoveryNodesJsonString = DataFetcher.fetchData(GET_NODES_TRIBE_URL_PREFIX + tribeId, logger);
            } else {
                discoveryNodesJsonString = DataFetcher.fetchData(GET_NODES_ISLAND_URL, logger);
            }

            List<RaigadInstance> instances = ElasticsearchUtil.getRaigadInstancesFromJsonString(discoveryNodesJsonString, logger);

            for (RaigadInstance instance : instances) {
                try {
                    TransportAddress[] addresses = transportService.addressesFromString(instance.getHostIP(), 1);

                    if (addresses != null && addresses.length > 0) {
                        logger.info("[raigad-discovery] Adding instance [{}] (address [{}], transport address [{}])",
                                instance.getId(), instance.getHostIP(), addresses[0]);

                        discoveryNodes.add(new DiscoveryNode(instance.getId(), addresses[0], Version.CURRENT.minimumCompatibilityVersion()));
                    }
                } catch (Exception e) {
                    logger.warn("[raigad-discovery] Failed to add instance [{}] (address [{}])", e, instance.getId(), instance.getHostIP());
                }
            }
        } catch (Exception e) {
            logger.error("[raigad-discovery] Exception while trying to build dynamic discovery nodes", e);
            throw new RuntimeException(e);
        }


        logger.debug("[raigad-discovery] Using dynamic discovery nodes {}", discoveryNodes);

        return discoveryNodes;
    }

    private boolean isTribeNode() {
        if (settings == null) {
            return false;
        }

        String tribeName = settings.get("name");

        if (tribeName == null || tribeName.isEmpty()) {
            return false;
        }

        return tribeName.contains("/t");
    }
}
