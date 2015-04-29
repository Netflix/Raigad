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
import com.netflix.raigad.backup.SnapshotBackupManager;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class SnapshotBackupMonitor extends Task
{

    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackupMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_SnapshotBackupMonitor";
    private final Elasticsearch_SnapshotBackupReporter snapshotBackupReporter;
    private final SnapshotBackupManager snapshotBackupManager;

    @Inject
    public SnapshotBackupMonitor(IConfiguration config,SnapshotBackupManager snapshotBackupManager)
    {
        super(config);
        snapshotBackupReporter = new Elasticsearch_SnapshotBackupReporter();
        this.snapshotBackupManager = snapshotBackupManager;
        Monitors.registerObject(snapshotBackupReporter);
    }

    @Override
    public void execute() throws Exception {

        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        SnapshotBackupBean snapshotBackupBean = new SnapshotBackupBean();
        try
        {
            snapshotBackupBean.snapshotSuccess = snapshotBackupManager.getNumSnapshotSuccess();
            snapshotBackupBean.snapshotFailure = snapshotBackupManager.getNumSnapshotFailure();
        }
        catch(Exception e)
        {
            logger.warn("failed to load Cluster SnapshotBackup Status", e);
        }

        snapshotBackupReporter.snapshotBackupBean.set(snapshotBackupBean);
    }

    public class Elasticsearch_SnapshotBackupReporter
    {
        private final AtomicReference<SnapshotBackupBean> snapshotBackupBean;

        public Elasticsearch_SnapshotBackupReporter()
        {
            snapshotBackupBean = new AtomicReference<SnapshotBackupBean>(new SnapshotBackupBean());
        }

        @Monitor(name="snapshot_success", type= DataSourceType.GAUGE)
        public int getSnapshotSuccess() {
            return snapshotBackupBean.get().snapshotSuccess;
        }

        @Monitor(name="snapshot_failure", type=DataSourceType.GAUGE)
        public int getSnapshotFailure() {
            return snapshotBackupBean.get().snapshotFailure;
        }

    }

    private static class SnapshotBackupBean
    {
        private int snapshotSuccess;
        private int snapshotFailure;
    }

    public static TaskTimer getTimer(String name)
    {
        return new SimpleTimer(name, 3600 * 1000);
    }

    @Override
    public String getName()
    {
        return METRIC_NAME;
    }

}
