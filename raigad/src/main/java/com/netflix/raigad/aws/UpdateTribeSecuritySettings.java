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

package com.netflix.raigad.aws;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.identity.IMembership;
import com.netflix.raigad.identity.IRaigadInstanceFactory;
import com.netflix.raigad.identity.InstanceManager;
import com.netflix.raigad.identity.RaigadInstance;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class will associate public IP's with a new instance so they can talk across the regions.
 * <p>
 * Requirements:
 * (1) Nodes in the same region needs to be able to talk to each other.
 * (2) Nodes in other regions needs to be able to talk to the others in the other region.
 * <p>
 * Assumptions:
 * (1) IRaigadInstanceFactory will provide the membership and will be visible across the regions
 * (2) IMembership Amazon or any other implementation which can tell if the instance
 * is part of the group (ASG in Amazon's case).
 */

@Singleton
public class UpdateTribeSecuritySettings extends Task {
    private static final Logger logger = LoggerFactory.getLogger(UpdateTribeSecuritySettings.class);

    public static final String JOB_NAME = "Update_TRIBE_SG";
    public static boolean firstTimeUpdated = false;
    private static final String COMMA_SEPARATOR = ",";
    private static final String PARAM_SEPARATOR = "=";

    private static final Random ran = new Random();
    private final IMembership membership;
    private final IRaigadInstanceFactory factory;

    /**
     * clusterPortMap
     * es_tribe : 8000
     * es_tribe_source1 : 8001
     * es_tribe_source2 : 8002
     */
    private final Map<String, Integer> clusterPortMap = new HashMap<String, Integer>();

    @Inject
    public UpdateTribeSecuritySettings(IConfiguration config, IMembership membership, IRaigadInstanceFactory factory) {
        super(config);
        this.membership = membership;
        this.factory = factory;
    }

    /**
     * Master nodes execute this at the specified interval.
     * Other nodes run only on startup.
     */
    @Override
    public void execute() {
        // Initializing cluster-port map from config properties
        initializeClusterPortMap();

        List<String> accessControlLists = new ArrayList<>();
        for (String clusterName : clusterPortMap.keySet()) {
            List<String> aclList = membership.listACL(clusterPortMap.get(clusterName), clusterPortMap.get(clusterName));
            accessControlLists.addAll(aclList);
        }

        List<RaigadInstance> instances = getInstanceList();
        Map<String, String> addAclClusterMap = new HashMap<>();
        Map<String, String> currentIpClusterMap = new HashMap<>();

        for (RaigadInstance instance : instances) {
            String range = instance.getHostIP() + "/32";
            if (!accessControlLists.contains(range)) {
                addAclClusterMap.put(range, instance.getApp());
            }

            // Just generating ranges
            currentIpClusterMap.put(range, instance.getApp());
        }

        if (addAclClusterMap.keySet().size() > 0) {
            /**
             * clusterInstancesMap
             * es_tribe : 50.60.70.80,50.60.70.81
             * es_tribe_source1 : 60.70.80.90,60.70.80.91
             * es_tribe_source2 : 70.80.90.00,70.80.90.01
             */
            Map<String, List<String>> clusterInstancesMap = generateClusterToAclListMap(addAclClusterMap);

            for (String currentClusterName : clusterInstancesMap.keySet()) {
                if (currentClusterName.startsWith("es_tribe_")) {
                    clusterPortMap.forEach((clusterName, transportPort) -> {
                        logger.info("Adding IPs for {} on port {}: {}", currentClusterName, transportPort, clusterInstancesMap.get(currentClusterName));
                        membership.addACL(clusterInstancesMap.get(currentClusterName), transportPort, transportPort);
                    });
                } else {
                    logger.info("Adding IPs for {} on port {}: {}", currentClusterName, clusterPortMap.get(currentClusterName), clusterInstancesMap.get(currentClusterName));
                    membership.addACL(clusterInstancesMap.get(currentClusterName), clusterPortMap.get(currentClusterName), clusterPortMap.get(currentClusterName));
                }
            }

            firstTimeUpdated = true;
        }

        // Iterating to remove ACL's
        List<String> removeAclList = new ArrayList<>();
        for (String acl : accessControlLists) {
            if (!currentIpClusterMap.containsKey(acl)) {
                removeAclList.add(acl);
            }
        }

        if (removeAclList.size() > 0) {
            for (String acl : removeAclList) {
                Map<String, List<Integer>> aclPortMap = membership.getACLPortMap(acl);
                int from = aclPortMap.get(acl).get(0);
                int to = aclPortMap.get(acl).get(1);
                membership.removeACL(Collections.singletonList(acl), from, to);
            }
            firstTimeUpdated = true;
        }
    }

    private void initializeClusterPortMap() {
        // Adding existing cluster-port mapping
        if (!clusterPortMap.containsKey(config.getAppName())) {
            clusterPortMap.put(config.getAppName(), config.getTransportTcpPort());
            logger.info("Adding cluster [{}:{}]", config.getAppName(), config.getTransportTcpPort());
        }

        String clusterParams = config.getCommaSeparatedSourceClustersForTribeNode();
        assert (clusterParams != null) : "Clusters parameters cannot be null";

        String[] clusters = StringUtils.split(clusterParams.trim(), COMMA_SEPARATOR);
        assert (clusters.length != 0) : "At least one cluster is needed";

        //Common settings
        for (String cluster : clusters) {
            String[] clusterPort = cluster.trim().split(PARAM_SEPARATOR);
            assert (clusterPort.length != 2) : "Cluster name or transport port is missing in configuration";

            if (!clusterPortMap.containsKey(clusterPort[0].trim())) {
                String sourceTribeClusterName = clusterPort[0].trim();
                Integer sourceTribeClusterPort = Integer.parseInt(clusterPort[1].trim());
                clusterPortMap.put(sourceTribeClusterName, sourceTribeClusterPort);
                logger.info("Adding cluster [{}:{}]", sourceTribeClusterName, sourceTribeClusterPort);
            }
        }
    }

    private Map<String, List<String>> generateClusterToAclListMap(Map<String, String> addAclClusterMap) {
        Map<String, List<String>> clusterAclsMap = new HashMap<>();

        for (String acl : addAclClusterMap.keySet()) {
            if (clusterAclsMap.containsKey(addAclClusterMap.get(acl))) {
                clusterAclsMap.get(addAclClusterMap.get(acl)).add(acl);
            } else {
                List<String> aclList = new ArrayList<>();
                aclList.add(acl);
                clusterAclsMap.put(addAclClusterMap.get(acl), aclList);
            }
        }

        return clusterAclsMap;
    }

    private List<RaigadInstance> getInstanceList() {
        List<RaigadInstance> instances = new ArrayList<>();

        for (String clusterName : clusterPortMap.keySet()) {
            instances.addAll(factory.getAllIds(clusterName));
        }

        if (config.isDebugEnabled()) {
            for (RaigadInstance instance : instances) {
                logger.debug(instance.toString());
            }
        }

        return instances;
    }

    public static TaskTimer getTimer(InstanceManager instanceManager) {
        return new SimpleTimer(JOB_NAME, 120 * 1000 + ran.nextInt(120 * 1000));
    }

    @Override
    public String getName() {
        return JOB_NAME;
    }
}