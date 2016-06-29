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

package com.netflix.raigad.defaultimpl;

import com.google.inject.Inject;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.utils.IElasticsearchTuner;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class StandardTuner implements IElasticsearchTuner {
    private static final Logger logger = LoggerFactory.getLogger(StandardTuner.class);

    private static final String COMMA_SEPARATOR = ",";
    private static final String PARAM_SEPARATOR = "=";

    protected final IConfiguration config;

    @Inject
    public StandardTuner(IConfiguration config) {
        this.config = config;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void writeAllProperties(String yamlLocation, String hostname) throws IOException {
        logger.info("Using configuration of type [{}]", config.getClass());

        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);

        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        map.put("cluster.name", config.getAppName());
        map.put("node.name", config.getEsNodeName());
        map.put("http.port", config.getHttpPort());
        map.put("path.data", config.getDataFileLocation());
        map.put("path.logs", config.getLogFileLocation());
        map.put("transport.tcp.port", config.getTransportTcpPort());

        if (config.isKibanaSetupRequired()) {
            map.put("http.cors.enabled", true);
            map.put("http.cors.allow-origin", "*");
        }

        if (config.amITribeNode()) {
            String clusterParams = config.getCommaSeparatedSourceClustersForTribeNode();
            assert (clusterParams != null) : "Source Clusters for tribe nodes cannot be null";

            String[] clusters = StringUtils.split(clusterParams, COMMA_SEPARATOR);
            assert (clusters.length != 0) : "One or more clusters needed";

            //Common settings
            for (int i = 0; i < clusters.length; i++) {
                String[] clusterPort = clusters[i].split(PARAM_SEPARATOR);
                assert (clusterPort.length != 2) : "Cluster name or transport port is missing in configuration";

                map.put("tribe.t" + i + ".cluster.name", clusterPort[0]);
                map.put("tribe.t" + i + ".transport.tcp.port", Integer.parseInt(clusterPort[1]));
                map.put("tribe.t" + i + ".discovery.type", config.getElasticsearchDiscoveryType());
                logger.info("Adding cluster [{}:{}]", clusterPort[0], clusterPort[1]);
            }

            map.put("node.master", false);
            map.put("node.data", false);

            if (config.isMultiDC()) {
                map.put("network.publish_host", config.getHostIP());
            }

            if (config.amIWriteEnabledTribeNode()) {
                map.put("tribe.blocks.write", false);
            }
            else {
                map.put("tribe.blocks.write", true);
            }

            if (config.amIMetadataEnabledTribeNode()) {
                map.put("tribe.blocks.metadata", false);
            }
            else {
                map.put("tribe.blocks.metadata", true);
            }

            map.put("tribe.on_conflict", "prefer_" + config.getTribePreferredClusterIdOnConflict());
        }
        else {
            map.put("discovery.type", config.getElasticsearchDiscoveryType());
            map.put("discovery.zen.minimum_master_nodes", config.getMinimumMasterNodes());
            map.put("index.number_of_shards", config.getNumOfShards());
            map.put("index.number_of_replicas", config.getNumOfReplicas());
            map.put("index.refresh_interval", config.getIndexRefreshInterval());

            /**
            NOTE: When using awareness attributes, shards will not be allocated to nodes that
            do not have values set for those attributes. Important in dedicated master nodes deployment
             */
            map.put("cluster.routing.allocation.awareness.attributes", config.getClusterRoutingAttributes());

            if (config.isShardPerNodeEnabled()) {
                map.put("index.routing.allocation.total_shards_per_node", config.getTotalShardsPerNode());
            }

            if (config.isMultiDC()) {
                map.put("node.rack_id", config.getDC());
                map.put("network.publish_host", config.getHostIP());
            } else if (config.amISourceClusterForTribeNodeInMultiDC()) {
                map.put("node.rack_id", config.getRac());
                map.put("network.publish_host", config.getHostIP());
            } else {
                map.put("node.rack_id", config.getRac());
            }

            // TODO: Create new tuner for ASG-based deployment
            // TODO: Need to come up with better algorithm for non-ASG based deployments
            if (config.isAsgBasedDedicatedDeployment()) {
                if (config.getASGName().toLowerCase().contains("master")) {
                    map.put("node.master", true);
                    map.put("node.data", false);
                }
                else if (config.getASGName().toLowerCase().contains("data")) {
                    map.put("node.master", false);
                    map.put("node.data", true);
                }
                else if (config.getASGName().toLowerCase().contains("search")) {
                    map.put("node.master", false);
                    map.put("node.data", false);
                }
            }
        }

        addExtraEsParams(map);

        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    public void addExtraEsParams(Map map) {
        String extraConfigParams = config.getExtraConfigParams();

        if (extraConfigParams == null) {
            logger.info("Updating YAML: no extra ES params");
            return;
        }

        String[] pairs = extraConfigParams.trim().split(COMMA_SEPARATOR);
        logger.info("Updating YAML: adding extra ES params");

        for (String pair1 : pairs) {
            String[] keyValue = pair1.trim().split(PARAM_SEPARATOR);

            String raigadKey = keyValue[0].trim();
            String esKey = keyValue[1].trim();
            String esValue = config.getEsKeyName(raigadKey);

            logger.info("Updating YAML: Raigad key [{}], Elasticsearch key [{}], value [{}]", raigadKey, esKey, esValue);

            if (raigadKey == null || esKey == null || esValue == null) {
                logger.error("One of the extra keys or values is null, skipping...");
                continue;
            }

            map.put(esKey, esValue);
        }
    }
}
