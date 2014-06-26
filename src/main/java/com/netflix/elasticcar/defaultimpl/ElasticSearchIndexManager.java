package com.netflix.elasticcar.defaultimpl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.elasticcar.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class ElasticSearchIndexManager {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchIndexManager.class);
    private static final int ES_MONITORING_INITIAL_DELAY = 180;
    private static final String SPACE_DELIMITER = " ";
    private static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    static ScheduledFuture<?> scheduledFuture;
    private static final AtomicBoolean isMasterNode = new AtomicBoolean(false);

    @Inject
    protected ElasticSearchIndexManager(IConfiguration config) {
        init(config);
    }

    private void init(IConfiguration config) {
        scheduledFuture = executor.scheduleWithFixedDelay(new IndexAllocator(config), ES_MONITORING_INITIAL_DELAY, 10, TimeUnit.MINUTES);
    }

    static class IndexAllocator implements Runnable {

        private final IConfiguration config;

        IndexAllocator(IConfiguration config) {
            this.config = config;
        }
        public void run() {
            try
            {
                logger.info("Running ElasticSearchIndexManager task ...");
                // If Elasticsearch is started then only start the Index Manager
                if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
                    String exceptionMsg = "Elasticsearch is not yet started, check back again later";
                    logger.info(exceptionMsg);
                    return;
                }

                if (!isMasterNode.get()) {
                    String URL =  "http://127.0.0.1:"+config.getHttpPort()+"/_cat/master";
                    String response = SystemUtils.runHttpGetCommand(URL);
                    //Split the response on Spaces to get IP
                    if(response == null || response.isEmpty()) {
                        logger.error("Response from URL : <"+URL+"> is Null or Empty, hence stopping the current running thread");
                        scheduledFuture.cancel(false);
                    }
                    String ip = response.split(SPACE_DELIMITER)[2];
                    if (ip == null || ip.isEmpty()) {
                        logger.error("ip from URL : <"+URL+"> is Null or Empty, hence stopping the current running thread");
                        scheduledFuture.cancel(false);
                    }
                    if (ip.equalsIgnoreCase(config.getHostIP()) || ip.equalsIgnoreCase(config.getHostLocalIP()))
                        isMasterNode.set(true);
                    else {
                        logger.info("Current node is not a Master Node, hence stopping the current running thread");
                        scheduledFuture.cancel(false);
                    }
                }

                if (isMasterNode.get()) {
                   logger.info("Current node is a Master Node. Now start Creating/Checking Indices.");
                }

            }
            catch(Exception e)
            {
                logger.warn("Exception thrown while checking whether it's safe to turn off cluster.routing.allocation.enable property or not", e);
            }
        }
    }

}
