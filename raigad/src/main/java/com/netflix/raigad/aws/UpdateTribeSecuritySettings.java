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
package com.netflix.raigad.aws;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.identity.RaigadInstance;
import com.netflix.raigad.identity.IRaigadInstanceFactory;
import com.netflix.raigad.identity.IMembership;
import com.netflix.raigad.identity.InstanceManager;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * this class will associate Public IP's with a new instance so they can talk
 * across the regions.
 *
 * Requirement: 1) Nodes in the same region needs to be able to talk to each
 * other. 2) Nodes in other regions needs to be able to talk to the others in
 * the other region.
 *
 * Assumption: 1) IRaigadInstanceFactory will provide the membership... and will
 * be visible across the regions 2) IMembership amazon or any other
 * implementation which can tell if the instance is part of the group (ASG in
 * amazons case).
 *
 */
@Singleton
public class UpdateTribeSecuritySettings extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(UpdateTribeSecuritySettings.class);
	public static final String JOBNAME = "Update_TRIBESG";
    public static boolean firstTimeUpdated = false;
    private static final String COMMA_SEPARATOR = ",";
    private static final String PARAM_SEPARATOR = "=";

    private static final Random ran = new Random();
    private final IMembership membership;
    private final IRaigadInstanceFactory factory;
    /**
     * clusterPortMap
     * es_tribe : 8000
     * es_tribesource1 : 9001
     * es_tribesource2 : 9002
     */
    private final Map<String,Integer> clusterPortMap = new HashMap<String,Integer>();

    @Inject
    public UpdateTribeSecuritySettings(IConfiguration config, IMembership membership, IRaigadInstanceFactory factory)
    {
        super(config);
        this.membership = membership;
        this.factory = factory;
    }

    /**
     * Master nodes execute this at the specified interval.
     * Other nodes run only on startup.
     */
    @Override
    public void execute()
    {
        //Initialize Cluster-Port map from Config properties
        initializeClusterPortMap();

        List<String> acls = Lists.newArrayList();
        for(String clusterName:clusterPortMap.keySet())
        {
           List<String> aclList = membership.listACL(clusterPortMap.get(clusterName),clusterPortMap.get(clusterName));
           acls.addAll(aclList);
        }

        List<RaigadInstance> instances = getInstanceList();

        Map<String,String> addAclClusterMap = new HashMap<String, String>();
        //iterate to add ...
        for (RaigadInstance instance : getInstanceList())
        {
            String range = instance.getHostIP() + "/32";
            if(!acls.contains(range))
                addAclClusterMap.put(range,instance.getApp());
        }

        if (addAclClusterMap.keySet().size() > 0)
        {
            /**
             * clusterInstancesMap
             * es_tribe : 50.60.70.80,50.60.70.81
             * es_tribesource1 : 60.70.80.90,60.70.80.91
             * es_tribesource2 : 70.80.90.00,70.80.90.01
             */
            Map<String,List<String>> clusterInstancesMap = generateClusterToAclListMap(addAclClusterMap);

            for(String clusterName : clusterInstancesMap.keySet())
                membership.addACL(clusterInstancesMap.get(clusterName), clusterPortMap.get(clusterName), clusterPortMap.get(clusterName));

            firstTimeUpdated = true;
        }

        Map<String,String> currentIpClusterMap = new HashMap<String, String>();
        //just iterate to generate ranges ...
        for (RaigadInstance instance : instances)
        {
            String range = instance.getHostIP() + "/32";
            currentIpClusterMap.put(range,instance.getApp());
        }

        //iterate to remove ...
        List<String> removeAclList = new ArrayList<String>();
        for(String acl:acls)
        {
            if(!currentIpClusterMap.containsKey(acl))
                removeAclList.add(acl);
        }

        if(removeAclList.size() > 0)
        {
            for(String acl:removeAclList)
            {
                Map<String,List<Integer>> aclPortMap = membership.getACLPortMap(acl);
                int from = aclPortMap.get(acl).get(0);
                int to = aclPortMap.get(acl).get(1);
                membership.removeACL(Collections.singletonList(acl),from,to);
            }
            firstTimeUpdated = true;
        }
    }

    private void initializeClusterPortMap()
    {
        //Add Existing cluster-port
        if(!clusterPortMap.containsKey(config.getAppName()))
        {
            clusterPortMap.put(config.getAppName(), config.getTransportTcpPort());
            logger.info("Adding Cluster = <{}> with Port = <{}>", config.getAppName(), config.getTransportTcpPort());
        }

        String clusterParams = config.getCommaSeparatedSourceClustersForTribeNode();
        assert (clusterParams != null) : "Clusters parameters can't be null";

        String[] clusters = StringUtils.split(clusterParams.trim(),COMMA_SEPARATOR);
        assert (clusters.length != 0) : "One or more clusters needed";

        //Common Settings
        for(int i=0; i< clusters.length;i++)
        {
            String[] clusterPort = clusters[i].trim().split(PARAM_SEPARATOR);
            assert (clusterPort.length != 2) : "Cluster Name or Transport Port is missing in configuration";

            if(!clusterPortMap.containsKey(clusterPort[0].trim()))
            {
                clusterPortMap.put(clusterPort[0].trim(), Integer.parseInt(clusterPort[1].trim()));
                logger.info("Adding Cluster = <{}> with Port = <{}>", clusterPort[0], clusterPort[1]);
            }
        }
    }

    private Map<String,List<String>> generateClusterToAclListMap(Map<String,String> addAclClusterMap )
    {
        Map<String,List<String>> clusterAclsMap = new HashMap<String,List<String>>();

        for(String acl:addAclClusterMap.keySet())
        {
            if (clusterAclsMap.containsKey(addAclClusterMap.get(acl)))
            {
                clusterAclsMap.get(addAclClusterMap.get(acl)).add(acl);
            }
            else
            {
                List<String> aclList = Lists.newArrayList();
                aclList.add(acl);
                clusterAclsMap.put(addAclClusterMap.get(acl),aclList);
            }
        }

        return clusterAclsMap;
    }

    private List<RaigadInstance> getInstanceList()
    {
        List<RaigadInstance> _instances = new ArrayList<RaigadInstance>();

        for(String clusterName:clusterPortMap.keySet())
        {
            _instances.addAll(factory.getAllIds(clusterName));
        }
        if(config.isDebugEnabled())
        {
            for(RaigadInstance instance:_instances)
                logger.debug(instance.toString());
        }
        return _instances;
    }

    public static TaskTimer getTimer(InstanceManager instanceManager)
    {
        return new SimpleTimer(JOBNAME, 120 * 1000 + ran.nextInt(120 * 1000));
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }
}
