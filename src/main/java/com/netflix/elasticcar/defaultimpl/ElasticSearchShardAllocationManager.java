package com.netflix.elasticcar.defaultimpl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.elasticcar.utils.SystemUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is not used currently anywhere in the code.
 * If needed, it can be added in initialization class {ElasticCarServer}
 */
@Singleton
public class ElasticSearchShardAllocationManager {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchShardAllocationManager.class);
    private static final int ES_MONITORING_INITIAL_DELAY = 180;
    private static final String DELAYED_TIMEOUT = "60000";
    private static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    static ScheduledFuture<?> scheduledFuture;
    private static final AtomicBoolean isShardAllocationEnabled = new AtomicBoolean(false);

    @Inject
    protected ElasticSearchShardAllocationManager(IConfiguration config) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.getAppName()).build();
        TransportClient localClient = new TransportClient(settings);
        localClient.addTransportAddress(new InetSocketTransportAddress(config.getHostIP(),config.getTransportTcpPort()));
        init(localClient);
    }

    private void init(TransportClient client) {

        scheduledFuture = executor.scheduleWithFixedDelay(new ShardAllocator(client), ES_MONITORING_INITIAL_DELAY, 60, TimeUnit.SECONDS);

    }

    static class ShardAllocator implements Runnable {

        private final TransportClient localClient;

        ShardAllocator(TransportClient client){
           this.localClient = client;
        }

        public void run() {
            try
            {
                logger.info("Running ElasticSearchShardAllocationManager task ...");
                // If Elasticsearch is started then only start the shard allocation
                if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
                    String exceptionMsg = "Elasticsearch is not yet started, check back again later";
                    logger.info(exceptionMsg);
                    return;
                }

                ClusterHealthStatus healthStatus = localClient.admin().cluster().prepareHealth().setTimeout(DELAYED_TIMEOUT).execute().get().getStatus();
            /*
                Following check means Shards are getting rebalanced
             */
                if (healthStatus != ClusterHealthStatus.GREEN)
                {
                    //Following block should execute only once
                    if(!isShardAllocationEnabled.get()) {
                        String response = SystemUtils.runHttpGetCommand("http://127.0.0.1:8080/Elasticcar/REST/v1/esadmin/shard_allocation_enable/transient");
                        logger.info("Response from REST call = [" + response + "]. Successfully Enabled cluster.routing.allocation.enable property.");
                        isShardAllocationEnabled.set(true);
                    }
                    logger.info("Shards are still getting rebalanced. Hence not disabling cluster.routing.allocation.enable property yet");
                    return;
                }

                String response = SystemUtils.runHttpGetCommand("http://127.0.0.1:8080/Elasticcar/REST/v1/esadmin/shard_allocation_disable/transient");

                logger.info("Response from REST call = ["+ response +"]. Successfully disabled cluster.routing.allocation.enable property.");
                //Closing TransportClient
                localClient.close();
                //Job is done, hence Cancel the Running Scheduled Job
                logger.info("Stopping the current running thread because cluster.routing.allocation.enable property is already disabled");
                scheduledFuture.cancel(false);
            }
            catch(Exception e)
            {
                logger.warn("Exception thrown while checking whether it's safe to turn off cluster.routing.allocation.enable property or not", e);
            }
        }
    }

}
