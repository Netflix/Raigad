package com.netflix.elasticcar.monitoring;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.identity.InstanceManager;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;
import com.netflix.elasticcar.utils.ESTransportClient;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.transport.TransportClient;
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

        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        HealthBean healthBean = new HealthBean();
        try
        {
            TransportClient esTransportClient = ESTransportClient.instance(config).getTransportClient();

            ClusterHealthStatus clusterHealthStatus = esTransportClient.admin().cluster().prepareHealth().setTimeout(MASTER_NODE_TIMEOUT).execute().get().getStatus();

            ClusterHealthResponse clusterHealthResponse = esTransportClient.admin().cluster().prepareHealth().execute().actionGet(MASTER_NODE_TIMEOUT);

            if (clusterHealthStatus == null) {
                logger.info("ClusterHealthStatus is null,hence returning (No Health).");
                return;
            }
            //Check if status = GREEN, YELLOW or RED
            if (clusterHealthStatus.name().equalsIgnoreCase("GREEN")) {
                healthBean.greenorredstatus = 0;
                healthBean.greenoryellowstatus = 0;
            }
            else if (clusterHealthStatus.name().equalsIgnoreCase("YELLOW")) {
                healthBean.greenoryellowstatus = 1;
                healthBean.greenorredstatus = 0;
            }
            else if (clusterHealthStatus.name().equalsIgnoreCase("RED")) {
                healthBean.greenorredstatus = 1;
                healthBean.greenoryellowstatus = 0;
            }

            if(config.isNodeMismatchWithDiscoveryEnabled())
                //Check if there is Node Mismatch between Discovery and ES
                healthBean.nodematch = (clusterHealthResponse.getNumberOfNodes() == instanceManager.getAllInstances().size()) ? 0 : 1;
            else {
                healthBean.nodematch = (clusterHealthResponse.getNumberOfNodes() == config.getDesiredNumberOfNodesInCluster()) ? 0 : 1;

            if(config.isEurekaHealthCheckEnabled())
                healthBean.eurekanodematch = (clusterHealthResponse.getNumberOfNodes() == discoveryClient.getInstancesByVipAddress(config.getVipAddressForEurekaHealthCheck(), false).size()) ? 0 : 1;
        }
        }
        catch(Exception e)
        {
            logger.warn("failed to load Cluster Health Status", e);
        }

        healthReporter.healthBean.set(healthBean);
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
    }

    private static class HealthBean
    {
        private int greenorredstatus = -1;
        private int greenoryellowstatus = -1;
        private int nodematch = -1;
        private int eurekanodematch = -1;
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

}
