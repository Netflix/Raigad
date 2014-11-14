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
package com.netflix.raigad.identity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.utils.RetryableCallable;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides the central place to create and consume the identity of
 * the instance
 * 
 */
@Singleton
public class InstanceManager {

	private static final Logger logger = LoggerFactory
			.getLogger(InstanceManager.class);
	private final IRaigadInstanceFactory instanceFactory;
    private final IMembership membership;
    private final IConfiguration config;
	private RaigadInstance myInstance;
	private List<RaigadInstance> instanceList;
    private static final String COMMA_SEPARATOR = ",";
    private static final String PARAM_SEPARATOR = "=";

	@Inject
	public InstanceManager(IRaigadInstanceFactory instanceFactory, IMembership membership,
			IConfiguration config) throws Exception {

		this.instanceFactory = instanceFactory;
		this.membership = membership;
		this.config = config;
		init();
	}

	private void init() throws Exception {
		logger.info("***Deregistering Dead Instance");
		new RetryableCallable<Void>() 
		{
			@Override
			public Void retriableCall() throws Exception 
			{
				deregisterInstance(instanceFactory,config);
				return null;
			}
		}.call();
		
		logger.info("***Registering Instance");
		myInstance = new RetryableCallable<RaigadInstance>()
		{
			@Override
			public RaigadInstance retriableCall() throws Exception
			{
				RaigadInstance instance = registerInstance(instanceFactory,config);
				return instance;
			}
		}.call();
		logger.info("RaigadInstance Details = "+myInstance.toString());
	}

	private RaigadInstance registerInstance(
			IRaigadInstanceFactory instanceFactory, IConfiguration config) throws Exception {
		return instanceFactory
				.create(config.getAppName(),
						config.getDC() + "." + config.getInstanceId(),
						config.getInstanceId(), config.getHostname(),
						config.getHostIP(), config.getRac(), config.getDC(), config.getASGName(), null);
	}

	private void deregisterInstance(
			IRaigadInstanceFactory instanceFactory, IConfiguration config) throws Exception {
	    final List<RaigadInstance> allInstances = getInstanceList();
	    List<String> asgInstances = membership.getRacMembership();
	    for (RaigadInstance dead : allInstances)
	    {
	      // test same region and is it is alive.
	    	  // TODO: Provide Config prop to choose same DC/Region
	      if (!dead.getAsg().equals(config.getASGName()) || !dead.getAvailabilityZone().equals(config.getRac()) || asgInstances.contains(dead.getInstanceId()))
	        continue;
	      logger.info("Found dead instances: " + dead.getInstanceId());
	      instanceFactory.delete(dead);
	    }
	}

	public RaigadInstance getInstance()
	{
		return myInstance;
	}
	
	public List<RaigadInstance> getAllInstances()
	{
		return getInstanceList();
	}

    private List<RaigadInstance> getInstanceList()
    {
        List<RaigadInstance> _instances = new ArrayList<RaigadInstance>();

        //Considering same cluster will not serve as a Tribe Node and Source Cluster for Tribe Node
        if(config.amITribeNode())
        {
            String clusterParams = config.getCommaSeparatedSourceClustersForTribeNode();
            assert (clusterParams != null) : "I am a tribe node but I need One or more source clusters";

            String[] clusters = StringUtils.split(clusterParams,COMMA_SEPARATOR);
            assert (clusters.length != 0) : "One or more clusters needed";

            List<String> sourceClusters = new ArrayList<String>();
            //Common Settings
            for(int i=0; i< clusters.length;i++)
            {
                String[] clusterPort = clusters[i].split(PARAM_SEPARATOR);
                assert (clusterPort.length != 2) : "Cluster Name or Transport Port is missing in configuration";

                sourceClusters.add(clusterPort[0]);
                logger.info("Adding Cluster = <{}> ",clusterPort[0]);
            }

            for(String sourceClusterName : sourceClusters)
                _instances.addAll(instanceFactory.getAllIds(sourceClusterName));

            logger.info("Printing TribeNode Related nodes ...");
            for(RaigadInstance instance:_instances)
                logger.info(instance.toString());
        }
        else
            _instances.addAll(instanceFactory.getAllIds(config.getAppName()));

//        if(config.amISourceClusterForTribeNode())
//        {
//            List<String> tribeClusters = new ArrayList<String>(Arrays.asList(StringUtils.split(config.getCommaSeparatedTribeClusterNames(), ",")));
//            assert (tribeClusters.size() != 0) : "I am a source cluster but I need One or more tribe clusters";
//
//            for(String tribeClusterName : tribeClusters)
//                _instances.addAll(instanceFactory.getAllIds(tribeClusterName));
//        }

        //Adding Current cluster
//        _instances.addAll(instanceFactory.getAllIds(config.getAppName()));

        if(config.isDebugEnabled())
        {
            for(RaigadInstance instance:_instances)
                logger.debug(instance.toString());
        }
        return _instances;
    }

    public boolean isMaster()
    {
        //For Non-dedicated deployments, Return True (Every Node can be a master)
        return (!config.isAsgBasedDedicatedDeployment() || config.getASGName().toLowerCase().contains("master"));
    }
}
