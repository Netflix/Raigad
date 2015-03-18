/**
 * Copyright 2014 Netflix, Inc.
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
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        map.put("cluster.name", config.getAppName());
        map.put("node.name", config.getRac() + "." + config.getInstanceId());
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
            assert (clusterParams != null) : "Source Clusters for Tribe Nodes can't be null";

            String[] clusters = StringUtils.split(clusterParams, COMMA_SEPARATOR);
            assert (clusters.length != 0) : "One or more clusters needed";

            //Common Settings
            for (int i = 0; i < clusters.length; i++) {
                String[] clusterPort = clusters[i].split(PARAM_SEPARATOR);
                assert (clusterPort.length != 2) : "Cluster Name or Transport Port is missing in configuration";

                map.put("tribe.t" + i + ".cluster.name", clusterPort[0]);
                map.put("tribe.t" + i + ".transport.tcp.port", Integer.parseInt(clusterPort[1]));
                map.put("tribe.t" + i + ".discovery.type", config.getElasticsearchDiscoveryType());
                logger.info("Adding Cluster = <{}> with Port = <{}>", clusterPort[0], clusterPort[1]);
            }

            map.put("node.master", false);
            map.put("node.data", false);

            if (config.amIWriteEnabledTribeNode())
                map.put("tribe.blocks.write", false);
            else
                map.put("tribe.blocks.write", true);

            if (config.amIMetadataEnabledTribeNode())
                map.put("tribe.blocks.metadata", false);
            else
                map.put("tribe.blocks.metadata", true);
        } else {
            map.put("discovery.type", config.getElasticsearchDiscoveryType());
            map.put("discovery.zen.minimum_master_nodes", config.getMinimumMasterNodes());
            map.put("index.number_of_shards", config.getNumOfShards());
            map.put("index.number_of_replicas", config.getNumOfReplicas());
            map.put("index.refresh_interval", config.getIndexRefreshInterval());
            //NOTE: When using awareness attributes, shards will not be allocated to nodes
            //that do not have values set for those attributes.
            //*** Important in dedicated master nodes deployment
            map.put("cluster.routing.allocation.awareness.attributes", config.getClusterRoutingAttributes());

            if (config.isShardPerNodeEnabled())
                map.put("index.routing.allocation.total_shards_per_node", config.getTotalShardsPerNode());

            if (config.isMultiDC()) {
                map.put("node.rack_id", config.getDC());
                map.put("network.publish_host", config.getHostIP());
            } else {
                map.put("node.rack_id", config.getRac());
            }

            //TODO: Create New Tuner for ASG Based Deployment
            //TODO: Need to come up with better algorithm for Non-ASG based deployments
            if (config.isAsgBasedDedicatedDeployment()) {
                if (config.getASGName().toLowerCase().contains("master")) {
                    map.put("node.master", true);
                    map.put("node.data", false);
                } else if (config.getASGName().toLowerCase().contains("data")) {
                    map.put("node.master", false);
                    map.put("node.data", true);
                } else if (config.getASGName().toLowerCase().contains("search")) {
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
        String params = config.getExtraConfigParams();
        if (params == null) {
            logger.info("Updating yaml: no extra ES params");
            return;
        }

        String[] pairs = params.trim().split(COMMA_SEPARATOR);
        logger.info("Updating yaml: adding extra ES params");
        for (int i = 0; i < pairs.length; i++) {
            String[] pair = pairs[i].trim().split(PARAM_SEPARATOR);
            String escarKey = pair[0].trim();
            String esKey = pair[1].trim();
            String esVal = config.getEsKeyName(escarKey);
            logger.info("Updating yaml: Raigadkey[" + escarKey + "], EsKey[" + esKey + "], Val[" + esVal + "]");
            if (escarKey == null || esKey == null || esVal == null) {
                logger.error("One of the Keys/values is null and hence not adding to yml and moving on ...");
                continue;
            }
            map.put(esKey, esVal);
        }
    }
}
