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

package com.netflix.raigad.identity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.utils.RetriableCallable;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * This class provides the central place to create and consume the identity of the instance
 */
@Singleton
public class InstanceManager {
	private static final Logger logger = LoggerFactory.getLogger(InstanceManager.class);

	private static final String COMMA_SEPARATOR = ",";
	private static final String PARAM_SEPARATOR = "=";

	private final IRaigadInstanceFactory instanceFactory;
	private final IMembership membership;
	private final IConfiguration config;
	private RaigadInstance thisInstance;

	@Inject
	public InstanceManager(IRaigadInstanceFactory instanceFactory, IMembership membership, IConfiguration config) throws Exception {
		this.instanceFactory = instanceFactory;
		this.membership = membership;
		this.config = config;
		init();
	}

	private void init() throws Exception {
		logger.info("Deregistering dead instances");
		new RetriableCallable<Void>() {
			@Override
			public Void retriableCall() throws Exception {
				deregisterInstance(instanceFactory, config);
				return null;
			}
		}.call();

		logger.info("Registering this instance");
		thisInstance = new RetriableCallable<RaigadInstance>() {
			@Override
			public RaigadInstance retriableCall() throws Exception {
				RaigadInstance instance = registerInstance(instanceFactory, config);
				return instance;
			}
		}.call();

		logger.info("Raigad instance details: " + thisInstance.toString());
	}

	private RaigadInstance registerInstance(IRaigadInstanceFactory instanceFactory, IConfiguration config) throws Exception {
		return instanceFactory.create(
				config.getAppName(),
				config.getDC() + "." + config.getInstanceId(),
				config.getInstanceId(), config.getHostname(),
				config.getHostIP(), config.getRac(), config.getDC(), config.getASGName(), null);
	}

	private void deregisterInstance(IRaigadInstanceFactory instanceFactory, IConfiguration config) throws Exception {
		final List<RaigadInstance> allInstances = getInstanceList();

		HashSet<String> asgNames = new HashSet<>();
		for (RaigadInstance raigadInstance : allInstances) {
			if (!asgNames.contains(raigadInstance.getAsg())) {
				asgNames.add(raigadInstance.getAsg());
			}
		}

		logger.info("Known instances: {}", allInstances);
		logger.info("Known ASG's: {}", StringUtils.join(asgNames, ","));

		Map<String, List<String>> instancesPerAsg = membership.getRacMembership(asgNames);

		logger.info("Known instances per ASG: {}", instancesPerAsg);
		for (RaigadInstance knownInstance : allInstances) {
			// Test same region and if it is alive.
			// TODO: Provide a config property to choose same DC/Region

			if (instancesPerAsg.containsKey(knownInstance.getAsg())) {
				if (!knownInstance.getAsg().equals(config.getASGName())) {
					logger.info("Skipping {} - different ASG", knownInstance.getInstanceId());
					continue;
				}
				if (!knownInstance.getAvailabilityZone().equals(config.getRac())) {
					logger.info("Skipping {} - different AZ", knownInstance.getInstanceId());
					continue;
				}
				if (instancesPerAsg.get(config.getASGName()).contains(knownInstance.getInstanceId())) {
					logger.info("Skipping {} - legitimate node", knownInstance.getInstanceId());
					continue;
				}

				logger.info("Found dead instance: " + knownInstance.getInstanceId());
				instanceFactory.delete(knownInstance);
			}
			else if (config.isMultiDC()) {
				logger.info("Multi DC setup, skipping unknown instances (" + knownInstance.getInstanceId() + ")");
			}
			else if (config.amISourceClusterForTribeNode()) {
				logger.info("Tribe setup, skipping unknown instances (" + knownInstance.getInstanceId() + ")");
			}
			else {
				logger.info("Found dead instance: " + knownInstance.getInstanceId());
				instanceFactory.delete(knownInstance);
			}
		}
	}

	public RaigadInstance getInstance() {
		return thisInstance;
	}

	public List<RaigadInstance> getAllInstances() {
		return getInstanceList();
	}

	private List<RaigadInstance> getInstanceList() {
		List<RaigadInstance> instances = new ArrayList<RaigadInstance>();

		// Considering same cluster will not serve as a tribe node and source cluster for the tribe node
		if (config.amITribeNode()) {
			String clusterParams = config.getCommaSeparatedSourceClustersForTribeNode();
			assert (clusterParams != null) : "I am a tribe node but I need one or more source clusters";

			String[] clusters = StringUtils.split(clusterParams, COMMA_SEPARATOR);
			assert (clusters.length != 0) : "One or more clusters needed";

			List<String> sourceClusters = new ArrayList<>();

			// Adding current cluster
			sourceClusters.add(config.getAppName());

			// Common settings
			for (int i = 0; i < clusters.length; i ++) {
				String[] clusterAndPort = clusters[i].split(PARAM_SEPARATOR);
				assert (clusterAndPort.length != 2) : "Cluster name or transport port is missing in configuration";
				sourceClusters.add(clusterAndPort[0]);
				logger.info("Adding cluster = <{}> ", clusterAndPort[0]);
			}

			for (String sourceClusterName : sourceClusters) {
				instances.addAll(instanceFactory.getAllIds(sourceClusterName));
			}

			logger.info("Printing tribe node related nodes...");

			for (RaigadInstance instance:instances) {
				logger.info(instance.toString());
			}
		}
		else {
			instances.addAll(instanceFactory.getAllIds(config.getAppName()));
		}

		if (config.isDebugEnabled()) {
			for (RaigadInstance instance : instances) {
				logger.debug(instance.toString());
			}
		}

		return instances;
	}

	public List<RaigadInstance> getAllInstancesPerCluster(String clusterName) {
		return getInstanceListPerCluster(clusterName);
	}

	private List<RaigadInstance> getInstanceListPerCluster(String clusterName) {
		List<RaigadInstance> instances = new ArrayList<RaigadInstance>();
		instances.addAll(instanceFactory.getAllIds(clusterName.trim().toLowerCase()));

		if (config.isDebugEnabled()) {
			for (RaigadInstance instance : instances) {
				logger.debug(instance.toString());
			}
		}

		return instances;
	}

	public boolean isMaster() {
		//For non-dedicated deployments, return true (every node can be a master)
		return (!config.isAsgBasedDedicatedDeployment() || config.getASGName().toLowerCase().contains("master"));
	}
}