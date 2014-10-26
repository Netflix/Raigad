/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.elasticcar.aws;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.identity.ElasticCarInstance;
import com.netflix.elasticcar.identity.IElasticCarInstanceFactory;
import com.netflix.elasticcar.identity.IMembership;
import com.netflix.elasticcar.identity.InstanceManager;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;
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
 * Assumption: 1) IElasticCarInstanceFactory will provide the membership... and will
 * be visible across the regions 2) IMembership amazon or any other
 * implementation which can tell if the instance is part of the group (ASG in
 * amazons case).
 *
 */
@Singleton
public class UpdateSecuritySettings extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(UpdateSecuritySettings.class);
	public static final String JOBNAME = "Update_SG";
    public static boolean firstTimeUpdated = false;

    private static final Random ran = new Random();
    private final IMembership membership;
    private final IElasticCarInstanceFactory factory;


    @Inject
    public UpdateSecuritySettings(IConfiguration config, IMembership membership, IElasticCarInstanceFactory factory)
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
        int port = config.getTransportTcpPort();
        List<String> acls = membership.listACL(port, port);

        //Get instances based on Type of Nodes (Tribe / non-tribe)
        List<ElasticCarInstance> instances = getInstanceList();

        // iterate to add...
        List<String> add = Lists.newArrayList();
        for (ElasticCarInstance instance : getInstanceList())
        {
            String range = instance.getHostIP() + "/32";
            if (!acls.contains(range))
                add.add(range);
        }

        if (add.size() > 0)
        {
            membership.addACL(add, port, port);
            firstTimeUpdated = true;
        }

        // just iterate to generate ranges.
        List<String> currentRanges = Lists.newArrayList();
        for (ElasticCarInstance instance : instances)
        {
            String range = instance.getHostIP() + "/32";
            currentRanges.add(range);
        }

        // iterate to remove...
        List<String> remove = Lists.newArrayList();
        for (String acl : acls)
            if (!currentRanges.contains(acl)) // if not found then remove....
                remove.add(acl);
        if (remove.size() > 0)
        {
            membership.removeACL(remove, port, port);
            firstTimeUpdated = true;
        }
    }

    private List<ElasticCarInstance> getInstanceList()
    {
        List<ElasticCarInstance> _instances = new ArrayList<ElasticCarInstance>();

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
            for(ElasticCarInstance instance:_instances)
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
