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
package com.netflix.raigad.monitoring;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.identity.InstanceManager;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import com.netflix.raigad.utils.ESTransportClient;
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class HealthMonitor extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(HealthMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_HealthMonitor";
    private final Elasticsearch_HealthReporter healthReporter;
    private final InstanceManager instanceManager;
    private static TimeValue MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(60);
    private final DiscoveryClient discoveryClient;

    @Inject
    public HealthMonitor(IConfiguration config,InstanceManager instanceManager)
    {
        super(config);
        this.instanceManager = instanceManager;
        healthReporter = new Elasticsearch_HealthReporter();
        discoveryClient = DiscoveryManager.getInstance().getDiscoveryClient();
        Monitors.registerObject(healthReporter);
    }

    @Override
    public void execute() throws Exception {

        HealthBean healthBean = new HealthBean();
        if (instanceManager.isMaster()) {
            reportClusterMetrics(healthBean);
        }
        healthBean.esuponinstance = ElasticsearchProcessMonitor.isElasticsearchStarted() ? 1 : 0;
        healthReporter.healthBean.set(healthBean);
    }

    private void reportClusterMetrics(HealthBean healthBean) {
        try {
            Client esTransportClient = ESTransportClient.instance(config).getTransportClient();

            ClusterHealthStatus clusterHealthStatus = esTransportClient.admin().cluster().prepareHealth().setTimeout(MASTER_NODE_TIMEOUT).execute().get().getStatus();

            ClusterHealthResponse clusterHealthResponse = esTransportClient.admin().cluster().prepareHealth().execute().actionGet(MASTER_NODE_TIMEOUT);

            if (clusterHealthStatus == null) {
                logger.info("ClusterHealthStatus is null, hence returning (No Health).");
                resetHealthStats(healthBean);
                return;
            }
            if ("YELLOW".equalsIgnoreCase(clusterHealthStatus.name())) {
                healthBean.greenoryellowstatus = 1;
            } else if ("RED".equalsIgnoreCase(clusterHealthStatus.name())) {
                healthBean.greenorredstatus = 1;
            }

            if (config.isNodeMismatchWithDiscoveryEnabled())
                //Check if there is Node Mismatch between Discovery and ES
                healthBean.nodematch = (clusterHealthResponse.getNumberOfNodes() == instanceManager.getAllInstances().size()) ? 0 : 1;
            else
                healthBean.nodematch = (clusterHealthResponse.getNumberOfNodes() == config.getDesiredNumberOfNodesInCluster()) ? 0 : 1;

            if (config.isEurekaHealthCheckEnabled())
                healthBean.eurekanodematch = (clusterHealthResponse.getNumberOfNodes() == discoveryClient.getApplication(config.getAppName()).getInstances().size()) ? 0 : 1;
        } catch (Exception e) {
            resetHealthStats(healthBean);
            logger.warn("failed to load Cluster Health Status", e);
        }
    }

    public class Elasticsearch_HealthReporter
    {
        private final AtomicReference<HealthBean> healthBean;

        public Elasticsearch_HealthReporter()
        {
            healthBean = new AtomicReference<HealthBean>(new HealthBean());
        }

        @Monitor(name ="es_healthstatus_greenorred", type=DataSourceType.GAUGE)
        public int getEsHealthstatusGreenorred()
        {
            return healthBean.get().greenorredstatus;
        }

        @Monitor(name ="es_healthstatus_greenoryellow", type=DataSourceType.GAUGE)
        public int getEsHealthstatusGreenoryellow()
        {
            return healthBean.get().greenoryellowstatus;
        }

        @Monitor(name ="es_nodematchstatus", type=DataSourceType.GAUGE)
        public int getEsNodematchstatus()
        {
            return healthBean.get().nodematch;
        }

        @Monitor(name ="es_eurekanodematchstatus", type=DataSourceType.GAUGE)
        public int getEsEurekanodematchstatus()
        {
            return healthBean.get().eurekanodematch;
        }

        @Monitor(name ="es_esuponinstance", type=DataSourceType.GAUGE)
        public int geEsUpOnInstance()
        {
            return healthBean.get().esuponinstance;
        }
    }

    private static class HealthBean
    {
        private int greenorredstatus = 0;
        private int greenoryellowstatus = 0;
        private int nodematch = 0;
        private int eurekanodematch = 0;
        private int esuponinstance = 0;
    }

    public static TaskTimer getTimer(String name)
    {
        return new SimpleTimer(name, 60 * 1000);
    }

    @Override
    public String getName()
    {
        return METRIC_NAME;
    }

    private void resetHealthStats(HealthBean healthBean){
        healthBean.greenorredstatus = 0;
        healthBean.greenoryellowstatus = 0;
        healthBean.nodematch = 0;
        healthBean.eurekanodematch = 0;
        healthBean.esuponinstance = 0;
    }
}
