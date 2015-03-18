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
package com.netflix.raigad.startup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.aws.UpdateSecuritySettings;
import com.netflix.raigad.aws.UpdateTribeSecuritySettings;
import com.netflix.raigad.backup.RestoreBackupManager;
import com.netflix.raigad.backup.SnapshotBackupManager;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.defaultimpl.IElasticsearchProcess;
import com.netflix.raigad.identity.InstanceManager;
import com.netflix.raigad.indexmanagement.ElasticSearchIndexManager;
import com.netflix.raigad.monitoring.*;
import com.netflix.raigad.scheduler.RaigadScheduler;
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import com.netflix.raigad.utils.HttpModule;
import com.netflix.raigad.utils.Sleeper;
import com.netflix.raigad.utils.TuneElasticsearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start all tasks here - Property update task - Backup task - Restore task -
 * Incremental backup
 */
@Singleton
public class RaigadServer {
    private final RaigadScheduler scheduler;
    private final IConfiguration config;
    private final Sleeper sleeper;
    private final IElasticsearchProcess esProcess;
    private final InstanceManager instanceManager;
    private final ElasticSearchIndexManager esIndexManager;
    private final SnapshotBackupManager snapshotBackupManager;
    private final HttpModule httpModule;
    private static final int ES_MONITORING_INITIAL_DELAY = 10;
    private static final int ES_SNAPSHOT_INITIAL_DELAY = 100;
    private static final int ES_HEALTH_MONITOR_DELAY = 600;
    private static final Logger logger = LoggerFactory.getLogger(RaigadServer.class);


    @Inject
    public RaigadServer(IConfiguration config, RaigadScheduler scheduler, HttpModule httpModule, IElasticsearchProcess esProcess, Sleeper sleeper,
                        InstanceManager instanceManager,
                        ElasticSearchIndexManager esIndexManager,
                        SnapshotBackupManager snapshotBackupManager) {
        this.config = config;
        this.scheduler = scheduler;
        this.httpModule = httpModule;
        this.esProcess = esProcess;
        this.sleeper = sleeper;
        this.instanceManager = instanceManager;
        this.esIndexManager = esIndexManager;
        this.snapshotBackupManager = snapshotBackupManager;
    }

    public void initialize() throws Exception {
        //Check If it's really needed
        if (instanceManager.getInstance().isOutOfService())
            return;

        logger.info("Initializing RaigadServer now ...");

        // start to schedule jobs
        scheduler.start();

        if (!config.isLocalModeEnabled()) {
            if (config.amITribeNode()) {
                // update security settings
                scheduler.runTaskNow(UpdateTribeSecuritySettings.class);
                // sleep for 60 sec for the SG update to happen.
                if (UpdateTribeSecuritySettings.firstTimeUpdated)
                    sleeper.sleep(60 * 1000);
                scheduler.addTask(UpdateTribeSecuritySettings.JOBNAME, UpdateTribeSecuritySettings.class, UpdateTribeSecuritySettings.getTimer(instanceManager));
            } else {
                if (config.isSecutrityGroupInMultiDC()) {
                    // update security settings
                    scheduler.runTaskNow(UpdateSecuritySettings.class);
                    // sleep for 60 sec for the SG update to happen.
                    if (UpdateSecuritySettings.firstTimeUpdated)
                        sleeper.sleep(60 * 1000);
                    scheduler.addTask(UpdateSecuritySettings.JOBNAME, UpdateSecuritySettings.class, UpdateSecuritySettings.getTimer(instanceManager));
                }
            }
        }

        // Tune Elasticsearch
        scheduler.runTaskNow(TuneElasticsearch.class);

        logger.info("Trying to start Elastic Search now ...");

        if (!config.doesElasticsearchStartManually()) {
            esProcess.start(true); // Start elasticsearch.
            if (config.isRestoreEnabled())
                scheduler.addTaskWithDelay(RestoreBackupManager.JOBNAME, RestoreBackupManager.class, RestoreBackupManager.getTimer(config), config.getRestoreTaskInitialDelayInSeconds());
        } else {
            logger.info("config.doesElasticsearchStartManually() is set to True, hence Elasticsearch needs to be started manually. Restore task needs to be started manually as well (if needed).");
        }

        /*
         *  Run the delayed task (after 10 seconds) to Monitor Elasticsearch Running Process
         */
        scheduler.addTaskWithDelay(ElasticsearchProcessMonitor.JOBNAME, ElasticsearchProcessMonitor.class, ElasticsearchProcessMonitor.getTimer(), ES_MONITORING_INITIAL_DELAY);

        /*
         *  Run Snapshot Backup task
         */
        if (config.isAsgBasedDedicatedDeployment()) {
            if (config.getASGName().toLowerCase().contains("master")) {   // Run Snapshot task only on Master Nodes
                scheduler.addTaskWithDelay(SnapshotBackupManager.JOBNAME, SnapshotBackupManager.class, SnapshotBackupManager.getTimer(config), ES_SNAPSHOT_INITIAL_DELAY);
                // Run Index Management task only on Master Nodes
                scheduler.addTaskWithDelay(ElasticSearchIndexManager.JOBNAME, ElasticSearchIndexManager.class, ElasticSearchIndexManager.getTimer(config), config.getAutoCreateIndexInitialStartDelaySeconds());
            }
        } else {
            scheduler.addTaskWithDelay(SnapshotBackupManager.JOBNAME, SnapshotBackupManager.class, SnapshotBackupManager.getTimer(config), ES_SNAPSHOT_INITIAL_DELAY);
            scheduler.addTaskWithDelay(ElasticSearchIndexManager.JOBNAME, ElasticSearchIndexManager.class, ElasticSearchIndexManager.getTimer(config), config.getAutoCreateIndexInitialStartDelaySeconds());
        }

        /*
        * Starting Monitoring Jobs
        */
        scheduler.addTask(ThreadPoolStatsMonitor.METRIC_NAME, ThreadPoolStatsMonitor.class, ThreadPoolStatsMonitor.getTimer("ThreadPoolStatsMonitor"));
        scheduler.addTask(TransportStatsMonitor.METRIC_NAME, TransportStatsMonitor.class, TransportStatsMonitor.getTimer("TransportStatsMonitor"));
        scheduler.addTask(NodeIndicesStatsMonitor.METRIC_NAME, NodeIndicesStatsMonitor.class, NodeIndicesStatsMonitor.getTimer("NodeIndicesStatsMonitor"));
        scheduler.addTask(FsStatsMonitor.METRIC_NAME, FsStatsMonitor.class, FsStatsMonitor.getTimer("FsStatsMonitor"));
        scheduler.addTask(NetworkStatsMonitor.METRIC_NAME, NetworkStatsMonitor.class, NetworkStatsMonitor.getTimer("NetworkStatsMonitor"));
        scheduler.addTask(JvmStatsMonitor.METRIC_NAME, JvmStatsMonitor.class, JvmStatsMonitor.getTimer("JvmStatsMonitor"));
        scheduler.addTask(OsStatsMonitor.METRIC_NAME, OsStatsMonitor.class, OsStatsMonitor.getTimer("OsStatsMonitor"));
        scheduler.addTask(ProcessStatsMonitor.METRIC_NAME, ProcessStatsMonitor.class, ProcessStatsMonitor.getTimer("ProcessStatsMonitor"));
        scheduler.addTask(HttpStatsMonitor.METRIC_NAME, HttpStatsMonitor.class, HttpStatsMonitor.getTimer("HttpStatsMonitor"));
        scheduler.addTask(AllCircuitBreakerStatsMonitor.METRIC_NAME, AllCircuitBreakerStatsMonitor.class, AllCircuitBreakerStatsMonitor.getTimer("AllCircuitBreakerStatsMonitor"));
        scheduler.addTask(SnapshotBackupMonitor.METRIC_NAME, SnapshotBackupMonitor.class, SnapshotBackupMonitor.getTimer("SnapshotBackupMonitor"));
        scheduler.addTaskWithDelay(HealthMonitor.METRIC_NAME, HealthMonitor.class, HealthMonitor.getTimer("HealthMonitor"), ES_HEALTH_MONITOR_DELAY);

    }

    public InstanceManager getInstanceManager() {
        return instanceManager;
    }

    public RaigadScheduler getScheduler() {
        return scheduler;
    }

    public IConfiguration getConfiguration() {
        return config;
    }

}
