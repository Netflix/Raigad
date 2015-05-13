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
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import com.netflix.raigad.utils.ESTransportClient;
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.monitor.fs.FsStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class FsStatsMonitor extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(FsStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_FsStatsMonitor";
    private final Elasticsearch_FsStatsReporter fsStatsReporter;

    @Inject
    public FsStatsMonitor(IConfiguration config)
    {
        super(config);
        fsStatsReporter = new Elasticsearch_FsStatsReporter();
        Monitors.registerObject(fsStatsReporter);
    }

    @Override
    public void execute() throws Exception {

        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        FsStatsBean fsStatsBean = new FsStatsBean();
        try
        {
            NodesStatsResponse ndsStatsResponse = ESTransportClient.getNodesStatsResponse(config);
            FsStats fsStats = null;
            NodeStats ndStat = null;
            if (ndsStatsResponse.getNodes().length > 0) {
                ndStat = ndsStatsResponse.getAt(0);
            }
            if (ndStat == null) {
                logger.info("NodeStats is null,hence returning (No FsStats).");
                return;
            }
            fsStats = ndStat.getFs();
            if (fsStats == null) {
                logger.info("FsStats is null,hence returning (No FsStats).");
                return;
            }

            fsStatsBean.total = fsStats.getTotal().getTotal().getBytes();
            fsStatsBean.free = fsStats.getTotal().getFree().getBytes();
            fsStatsBean.available = fsStats.getTotal().getAvailable().getBytes();
            fsStatsBean.diskReads = fsStats.getTotal().getDiskReads();
            fsStatsBean.diskWrites = fsStats.getTotal().getDiskWrites();
            fsStatsBean.diskReadBytes = fsStats.getTotal().getDiskReadSizeInBytes();
            fsStatsBean.diskWriteBytes = fsStats.getTotal().getDiskWriteSizeInBytes();
            fsStatsBean.diskQueue = fsStats.getTotal().getDiskQueue();
            fsStatsBean.diskServiceTime = fsStats.getTotal().getDiskServiceTime();
            fsStatsBean.availableDiskPercent =  (fsStatsBean.available * 100) / fsStatsBean.total;
        }
        catch(Exception e)
        {
            logger.warn("failed to load Fs stats data", e);
        }

        fsStatsReporter.fsStatsBean.set(fsStatsBean);
    }

    public class Elasticsearch_FsStatsReporter
    {
        private final AtomicReference<FsStatsBean> fsStatsBean;

        public Elasticsearch_FsStatsReporter()
        {
            fsStatsBean = new AtomicReference<FsStatsBean>(new FsStatsBean());
        }

        @Monitor(name ="total_bytes", type=DataSourceType.GAUGE)
        public long getTotalBytes()
        {
            return fsStatsBean.get().total;
        }
        @Monitor(name ="free_bytes", type=DataSourceType.GAUGE)
        public long getFreeBytes()
        {
            return fsStatsBean.get().free;
        }
        @Monitor(name ="available_bytes", type=DataSourceType.GAUGE)
        public long getAvailableBytes()
        {
            return fsStatsBean.get().available;
        }
        @Monitor(name ="disk_reads", type=DataSourceType.GAUGE)
        public long geDiskReads()
        {
            return fsStatsBean.get().diskReads;
        }
        @Monitor(name ="disk_writes", type=DataSourceType.GAUGE)
        public long getDiskWrites()
        {
            return fsStatsBean.get().diskWrites;
        }
        @Monitor(name ="disk_read_bytes", type=DataSourceType.GAUGE)
        public long getDiskReadBytes()
        {
            return fsStatsBean.get().diskReadBytes;
        }
        @Monitor(name ="disk_write_bytes", type=DataSourceType.GAUGE)
        public long getDiskWriteBytes()
        {
            return fsStatsBean.get().diskWriteBytes;
        }
        @Monitor(name ="disk_queue", type=DataSourceType.GAUGE)
        public double getDiskQueue()
        {
            return fsStatsBean.get().diskQueue;
        }
        @Monitor(name ="disk_service_time", type=DataSourceType.GAUGE)
        public double getDiskServiceTime()
        {
            return fsStatsBean.get().diskServiceTime;
        }
        @Monitor(name ="available_disk_percent", type=DataSourceType.GAUGE)
        public long getAvailableDiskPercent()
        {
            return fsStatsBean.get().availableDiskPercent;
        }

    }

    private static class FsStatsBean
    {
        private long total;
        private long free;
        private long available;
        private long diskReads;
        private long diskWrites;
        private long diskReadBytes;
        private long diskWriteBytes;
        private double diskQueue;
        private double diskServiceTime;
        private long availableDiskPercent;
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