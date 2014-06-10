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
package com.netflix.elasticcar;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.aws.UpdateSecuritySettings;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.identity.InstanceManager;
import com.netflix.elasticcar.monitoring.*;
import com.netflix.elasticcar.scheduler.ElasticCarScheduler;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.elasticcar.utils.Sleeper;
import com.netflix.elasticcar.utils.TuneElasticsearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start all tasks here - Property update task - Backup task - Restore task -
 * Incremental backup
 */
@Singleton
public class ElasticCarServer
{
    private final ElasticCarScheduler scheduler;
    private final IConfiguration config;
    private final Sleeper sleeper;
    private final IElasticsearchProcess esProcess;
    private final InstanceManager instanceManager;
    private static final int ES_MONITORING_INITIAL_DELAY = 10;
    private static final Logger logger = LoggerFactory.getLogger(ElasticCarServer.class);


    @Inject
    public ElasticCarServer(IConfiguration config, ElasticCarScheduler scheduler, IElasticsearchProcess esProcess, Sleeper sleeper, InstanceManager instanceManager)
    {
        this.config = config;
        this.scheduler = scheduler;
        this.esProcess = esProcess;
        this.sleeper = sleeper;
        this.instanceManager = instanceManager;
    }
    
    public void intialize() throws Exception
    {     
    		//Check If it's really needed
        if (instanceManager.getInstance().isOutOfService())
            return;
        
        logger.info("Initializing ElasticCarServer now ...");

        // start to schedule jobs
        scheduler.start();

//        // update security settings.
//        if (config.isMultiDC())
//        {
//            scheduler.runTaskNow(UpdateSecuritySettings.class);
//            // sleep for 60 sec for the SG update to happen.
//            if (UpdateSecuritySettings.firstTimeUpdated)
//                sleeper.sleep(60 * 1000);
//            scheduler.addTask(UpdateSecuritySettings.JOBNAME, UpdateSecuritySettings.class, UpdateSecuritySettings.getTimer(id));
//        }
//
        
        scheduler.runTaskNow(UpdateSecuritySettings.class);
        
        if (UpdateSecuritySettings.firstTimeUpdated)
          sleeper.sleep(60 * 1000);
        
        scheduler.addTask(UpdateSecuritySettings.JOBNAME, UpdateSecuritySettings.class, UpdateSecuritySettings.getTimer());

        
        scheduler.runTaskNow(TuneElasticsearch.class);
        
        logger.info("Trying to start Elastic Search now ...");
        
		if (!config.doesElasticsearchStartManually())
			esProcess.start(true); // Start elasticsearch.
		else
			logger.info("config.doesElasticsearchStartManually() is set to True, hence Elasticsearch needs to be started manually ...");        

        /*
         *  Run the delayed task (after 10 seconds) to Monitor Elasticsearch Running Process
         */
        scheduler.addTaskWithDelay(ElasticsearchProcessMonitor.JOBNAME,ElasticsearchProcessMonitor.class, ElasticsearchProcessMonitor.getTimer(), ES_MONITORING_INITIAL_DELAY);

//        if(config.isCustomShardAllocationPolicyEnabled())
//            scheduler.addTask(ElasticSearchShardAllocationManager.JOBNAME, ElasticSearchShardAllocationManager.class, ElasticSearchShardAllocationManager.getTimer());
        /*
        * Starting Monitoring Jobs
        */
        scheduler.addTask(ThreadPoolStatsMonitor.METRIC_NAME, ThreadPoolStatsMonitor.class, ThreadPoolStatsMonitor.getTimer("ThreadPoolStatsMonitor"));
        scheduler.addTask(TransportStatsMonitor.METRIC_NAME, TransportStatsMonitor.class, TransportStatsMonitor.getTimer("TransportStatsMonitor"));
        scheduler.addTask(NodeIndicesStatsMonitor.METRIC_NAME, NodeIndicesStatsMonitor.class, NodeIndicesStatsMonitor.getTimer("NodeIndicesStatsMonitor"));
        scheduler.addTask(FsStatsMonitor.METRIC_NAME, FsStatsMonitor.class, FsStatsMonitor.getTimer("FsStatsMonitor"));
        scheduler.addTask(NetworkStatsMonitor.METRIC_NAME, NetworkStatsMonitor.class, NetworkStatsMonitor.getTimer("NetworkStatsMonitor"));      
    }

    public InstanceManager getInstanceManager()
    {
        return instanceManager;
    }

    public ElasticCarScheduler getScheduler()
    {
        return scheduler;
    }

    public IConfiguration getConfiguration()
    {
        return config;
    }

}
