package com.netflix.elasticcar.monitoring;

import com.google.inject.Inject;
import com.google.inject.Singleton;
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
    private static final String MATCH_TAG = "MATCH";
    private static final String MISMATCH_TAG = "MISMATCH";
    private final Elasticsearch_HealthReporter healthReporter;
    private final InstanceManager instanceManager;
    private static TimeValue MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(60);

    @Inject
    public HealthMonitor(IConfiguration config,InstanceManager instanceManager)
    {
        super(config);
        this.instanceManager = instanceManager;
        healthReporter = new Elasticsearch_HealthReporter();
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
            //Set status to GREEN, YELLOW or RED
            healthBean.status =  clusterHealthStatus.name();
            //Check if there is Node Mismatch between Discovery and ES
            healthBean.nodeMismatch = (clusterHealthResponse.getNumberOfNodes() == instanceManager.getAllInstances().size()) ? MATCH_TAG : MISMATCH_TAG;
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

        @Monitor(name ="es_healthstatus", type=DataSourceType.INFORMATIONAL)
        public String getEsHealthstatus()
        {
            return healthBean.get().status;
        }

        @Monitor(name ="es_nodemismatchstatus", type=DataSourceType.INFORMATIONAL)
        public String getEsNodemismatchstatus()
        {
            return healthBean.get().nodeMismatch;
        }

    }

    private static class HealthBean
    {
        private String status = "";
        private String nodeMismatch = "";
    }

    public static TaskTimer getTimer(String name)
    {
        return new SimpleTimer(name, 30 * 1000);
    }

    @Override
    public String getName()
    {
        return METRIC_NAME;
    }

}
