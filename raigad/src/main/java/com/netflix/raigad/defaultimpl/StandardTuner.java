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
import java.util.*;

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

        if (config.isVPCExternal()) {
            map.put("network.publish_host", config.getHostIP());
            map.put("http.publish_host", config.getHostname());
        }
        else {
            map.put("network.publish_host", "_global_");
        }

        if (config.isKibanaSetupRequired()) {
            map.put("http.cors.enabled", true);
            map.put("http.cors.allow-origin", "*");
        }

        if (config.amITribeNode()) {
            String clusterParams = config.getCommaSeparatedSourceClustersForTribeNode();
            assert (clusterParams != null) : "Source clusters for tribe nodes cannot be null";

            String[] clusters = StringUtils.split(clusterParams, COMMA_SEPARATOR);
            assert (clusters.length != 0) : "At least one source cluster is needed";

            List<Integer> tribePorts = new ArrayList<>();
            tribePorts.add(config.getTransportTcpPort());

            //Common settings
            for (int i = 0; i < clusters.length; i++) {
                String[] clusterNameAndPort = clusters[i].split(PARAM_SEPARATOR);
                assert (clusterNameAndPort.length != 2) : "Cluster name or transport port is missing in configuration";
                assert (StringUtils.isNumeric(clusterNameAndPort[1])) : "Source tribe cluster port is invalid";

                map.put("tribe.t" + i + ".cluster.name", clusterNameAndPort[0]);
                map.put("tribe.t" + i + ".transport.tcp.port", Integer.parseInt(clusterNameAndPort[1]));
                map.put("tribe.t" + i + ".network.host", "_global_");
                logger.info("Adding cluster [{}:{}]", clusterNameAndPort[0], clusterNameAndPort[1]);

                tribePorts.add(Integer.valueOf(clusterNameAndPort[1]));
            }

            Collections.sort(tribePorts);
            String transportPortRange = String.format("%d-%d", tribePorts.get(0), tribePorts.get(tribePorts.size() - 1));
            logger.info("Setting tribe transport port range to {}", transportPortRange);

            // Adding port range to include tribe cluster port as well as transport for each source cluster
            map.put("transport.tcp.port", transportPortRange);

            map.put("node.master", false);
            map.put("node.data", false);

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
            map.put("transport.tcp.port", config.getTransportTcpPort());

            map.put("discovery.zen.minimum_master_nodes", config.getMinimumMasterNodes());

            /**
            NOTE: When using awareness attributes, shards will not be allocated to nodes that
            do not have values set for those attributes. Important in dedicated master nodes deployment
             */
            map.put("cluster.routing.allocation.awareness.attributes", config.getClusterRoutingAttributes());

            if (config.isShardPerNodeEnabled()) {
                map.put("cluster.routing.allocation.total_shards_per_node", config.getTotalShardsPerNode());
            }

            if (config.isMultiDC()) {
                map.put("node.attr.rack_id", config.getDC());
            }
            else {
                map.put("node.attr.rack_id", config.getRac());
            }

            if (config.isAsgBasedDedicatedDeployment()) {
                if ("master".equalsIgnoreCase(config.getStackName())) {
                    map.put("node.master", true);
                    map.put("node.data", false);
                }
                else if ("data".equalsIgnoreCase(config.getStackName())) {
                    map.put("node.master", false);
                    map.put("node.data", true);
                }
                else if ("search".equalsIgnoreCase(config.getStackName())) {
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
            logger.info("Updating elasticsearch.yml: no extra parameters");
            return;
        }

        String[] pairs = extraConfigParams.trim().split(COMMA_SEPARATOR);
        logger.info("Updating elasticsearch.yml: adding extra parameters");

        for (String pair : pairs) {
            String[] keyValue = pair.trim().split(PARAM_SEPARATOR);

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
