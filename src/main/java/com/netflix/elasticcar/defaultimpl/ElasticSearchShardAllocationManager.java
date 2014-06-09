package com.netflix.elasticcar.defaultimpl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.elasticcar.utils.SystemUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ElasticSearchShardAllocationManager extends Task {

    public static final String JOBNAME = "ES_SHARDALLOCATOR_THREAD";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchShardAllocationManager.class);

    @Inject
    protected ElasticSearchShardAllocationManager(IConfiguration config) {
        super(config);
    }

    @Override
    public void execute() throws Exception {

        try
        {
            logger.info("Running ElasticSearchShardAllocationManager task ...");
            // If Elasticsearch is started then only start the shard allocation
            if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
                String exceptionMsg = "Elasticsearch is not yet started, check back again later";
                logger.info(exceptionMsg);
                return;
            }

            Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.getAppName()).build();
            TransportClient localClient = new TransportClient(settings);
            localClient.addTransportAddress(new InetSocketTransportAddress(config.getHostIP(),config.getTransportTcpPort()));

            ClusterHealthStatus healthStatus = localClient.admin().cluster().prepareHealth().setTimeout("1000").execute().get().getStatus();
            /*
                Following check means Shards are getting rebalanced
             */
            if (healthStatus != ClusterHealthStatus.GREEN)
            {
                logger.info("Shards are still getting rebalanced. Hence not disabling cluster.routing.allocation.enable property");
                return;
            }

            String response = SystemUtils.runHttpGetCommand("http://127.0.0.1:8080/Elasticcar/REST/v1/esadmin/shard_allocation_disable/transient");

            logger.info("Response from REST call = ["+ response +"]. Successfully disabled cluster.routing.allocation.enable property.");
            //Closing TransportClient
            localClient.close();
            //Job is done, hence Interrupt the current thread
            logger.info("Interrupting the current running thread because cluster.routing.allocation.enable property is already disabled");
            Thread.currentThread().interrupt();
        }
        catch(Exception e)
        {
            logger.warn("Exception thrown while checking whether it's safe to turn off cluster.routing.allocation.enable property or not", e);
        }

    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 10L * 1000);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

}
