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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.aws.UpdateSecuritySettings;
//import com.netflix.elasticcar.identity.InstanceIdentity;
import com.netflix.elasticcar.scheduler.ElasticCarScheduler;
import com.netflix.elasticcar.utils.Sleeper;
import com.netflix.elasticcar.utils.TuneElasticsearch;

/**
 * Start all tasks here - Property update task - Backup task - Restore task -
 * Incremental backup
 */
@Singleton
public class ElasticCarServer
{
    private final ElasticCarScheduler scheduler;
    private final IConfiguration config;
//    private final InstanceIdentity id;
//    private final Sleeper sleeper;
    private final IElasticsearchProcess esProcess;
    private static final Logger logger = LoggerFactory.getLogger(ElasticCarServer.class);

//    @Inject
//    public ElasticCarServer(IConfiguration config, ElasticCarScheduler scheduler, InstanceIdentity id, Sleeper sleeper, IElasticsearchProcess esProcess)
//    {
//        this.config = config;
//        this.scheduler = scheduler;
//        this.id = id;
//        this.sleeper = sleeper;
//        this.esProcess = esProcess;
//    }

    @Inject
    public ElasticCarServer(IConfiguration config, ElasticCarScheduler scheduler, IElasticsearchProcess esProcess)
    {
        this.config = config;
        this.scheduler = scheduler;
        this.esProcess = esProcess;    	
    }
    
    public void intialize() throws Exception
    {     
//        if (id.getInstance().isOutOfService())
//            return;
        
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
        scheduler.runTaskNow(TuneElasticsearch.class);
        
        logger.info("Trying to start Elastic Search now ...");
        
		if (!config.doesElasticsearchStartManually())
			esProcess.start(true); // Start elasticsearch.
		else
			logger.info("config.doesElasticsearchStartManually() is set to True, hence Elasticsearch needs to be started manually ...");        

       
    }

//    public InstanceIdentity getId()
//    {
//        return id;
//    }
//
//    public ElasticCarScheduler getScheduler()
//    {
//        return scheduler;
//    }

    public IConfiguration getConfiguration()
    {
        return config;
    }

}
