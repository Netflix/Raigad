/**
 * Copyright 2016 Netflix, Inc.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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
public class UpdateVPCSecuritySettings extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(UpdateVPCSecuritySettings.class);
	public static final String JOBNAME = "Update_VPC_SG";
    public static boolean firstTimeUpdated = false;

    private static final Random ran = new Random();
    private final IMembership membership;
    private final IRaigadInstanceFactory factory;

    @Inject
    public UpdateVPCSecuritySettings(IConfiguration config, IMembership membership, IRaigadInstanceFactory factory)
    {
        super(config);
        this.membership = membership;
        this.factory = factory;
    }

    /**
     * Master nodes execute this at the specified interval, others run only on startup
     */
    @Override
    public void execute()
    {
        int port = config.getTransportTcpPort();
        List<String> acls = membership.listACL(port, port);

        // Get instances based on node types (tribe / non-tribe)
        List<RaigadInstance> instances = getInstanceList();

        // Iterate cluster nodes and build a list of IP's
        List<String> ipsToAdd = Lists.newArrayList();
        for (RaigadInstance instance : getInstanceList()) {
            String range = instance.getHostIP() + "/32";
            if (!acls.contains(range)) {
                ipsToAdd.add(range);
            }
        }

        if (ipsToAdd.size() > 0) {
            membership.addACL(ipsToAdd, port, port);
            firstTimeUpdated = true;
        }

        // Just iterate to generate ranges
        List<String> currentRanges = Lists.newArrayList();
        for (RaigadInstance instance : instances) {
            String range = instance.getHostIP() + "/32";
            currentRanges.add(range);
        }

        // Create a list of IP's to remove
        List<String> ipsToRemove = Lists.newArrayList();
        for (String acl : acls) {
            // Remove if not found
            if (!currentRanges.contains(acl)) {
                ipsToRemove.add(acl);
            }
        }

        if (ipsToRemove.size() > 0)
        {
            membership.removeACL(ipsToRemove, port, port);
            firstTimeUpdated = true;
        }
    }

    private List<RaigadInstance> getInstanceList()
    {
        List<RaigadInstance> _instances = new ArrayList<RaigadInstance>();

        if(config.amISourceClusterForTribeNode())
        {
            List<String> tribeClusters = new ArrayList<String>(Arrays.asList(StringUtils.split(config.getCommaSeparatedTribeClusterNames(), ",")));
            assert (tribeClusters.size() != 0) : "I am a source cluster but I need One or more tribe clusters";

            for(String tribeClusterName : tribeClusters)
                 _instances.addAll(factory.getAllIds(tribeClusterName));
        }

        //Adding Current cluster
        _instances.addAll(factory.getAllIds(config.getAppName()));

        if(config.isDebugEnabled())
        {
            for(RaigadInstance instance:_instances)
                logger.debug(instance.toString());
        }
        return _instances;
    }

    public static TaskTimer getTimer(InstanceManager instanceManager)
    {
        //Only Master nodes will Update Security Group Settings
        if(!instanceManager.isMaster())
            return new SimpleTimer(JOBNAME);
        else
            return new SimpleTimer(JOBNAME, 120 * 1000 + ran.nextInt(120 * 1000));
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }
}
