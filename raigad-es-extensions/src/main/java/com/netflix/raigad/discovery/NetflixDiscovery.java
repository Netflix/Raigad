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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class NetflixDiscovery extends ZenDiscovery {
    public static final String DISCOVERY_TYPE = "netflix";
    public static final String DISCOVERY_TYPE_LOCATION = "discovery.type";
    public static final String DISCOVERY_DESCRIPTION = "Netflix custom discovery plugin";

    public static final String GET_NODES_ISLAND_URL = "http://127.0.0.1:8080/Raigad/REST/v1/esconfig/get_nodes";
    public static final String GET_NODES_TRIBE_URL_PREFIX = "http://127.0.0.1:8080/Raigad/REST/v1/esconfig/get_tribe_nodes/";

    @Inject
    public NetflixDiscovery(Settings settings,
                            ClusterName clusterName,
                            ThreadPool threadPool,
                            TransportService transportService,
                            ClusterService clusterService,
                            NodeSettingsService nodeSettingsService,
                            ZenPingService pingService,
                            ElectMasterService electMasterService,
                            DiscoverySettings discoverySettings) {
        super(settings, clusterName, threadPool, transportService, clusterService,
                nodeSettingsService, pingService, electMasterService, discoverySettings);
    }
}
