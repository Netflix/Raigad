/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.elasticsearch.monitor.process.ProcessStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class ProcessStatsMonitor extends Task {
    private static final Logger logger = LoggerFactory.getLogger(ProcessStatsMonitor.class);

    public static final String METRIC_NAME = "Elasticsearch_ProcessStatsMonitor";
    private final Elasticsearch_ProcessStatsReporter processStatsReporter;

    @Inject
    public ProcessStatsMonitor(IConfiguration config) {
        super(config);
        processStatsReporter = new Elasticsearch_ProcessStatsReporter();
        Monitors.registerObject(processStatsReporter);
    }

    @Override
    public void execute() throws Exception {
        // Only start monitoring if Elasticsearch is started
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        ProcessStatsBean processStatsBean = new ProcessStatsBean();

        try {
            NodesStatsResponse nodesStatsResponse = ESTransportClient.getNodesStatsResponse(config);
            ProcessStats processStats = null;
            NodeStats nodeStats = null;

            if (nodesStatsResponse.getNodes().length > 0) {
                nodeStats = nodesStatsResponse.getAt(0);
            }

            if (nodeStats == null) {
                logger.info("Process stats is not available (node stats is not available)");
                return;
            }

            processStats = nodeStats.getProcess();
            if (processStats == null) {
                logger.info("Process stats is not available");
                return;
            }

            //Memory
            // TODO: 2X: Determine if this is necessary and if yes find an alternative
            //processStatsBean.residentInBytes = processStats.getMem().getResident().getBytes();
            //processStatsBean.shareInBytes = processStats.getMem().getShare().getBytes();
            processStatsBean.totalVirtualInBytes = processStats.getMem().getTotalVirtual().getBytes();

            //CPU
            processStatsBean.cpuPercent = processStats.getCpu().getPercent();
            processStatsBean.totalInMillis = processStats.getCpu().getTotal().getMillis();
            // TODO: 2X: Determine if this is necessary and if yes find an alternative
            //processStatsBean.sysInMillis = processStats.getCpu().getSys().getMillis();
            //processStatsBean.userInMillis = processStats.getCpu().getUser().getMillis();

            //Open file descriptors
            processStatsBean.openFileDescriptors = processStats.getOpenFileDescriptors();

            //Timestamp
            processStatsBean.cpuTimestamp = processStats.getTimestamp();
        }
        catch (Exception e) {
            logger.warn("Failed to load process stats data", e);
        }

        processStatsReporter.processStatsBean.set(processStatsBean);
    }

    public class Elasticsearch_ProcessStatsReporter {
        private final AtomicReference<ProcessStatsBean> processStatsBean;

        public Elasticsearch_ProcessStatsReporter() {
            processStatsBean = new AtomicReference<ProcessStatsBean>(new ProcessStatsBean());
        }

        @Monitor(name = "resident_in_bytes", type = DataSourceType.GAUGE)
        public long getResidentInBytes() {
            return processStatsBean.get().residentInBytes;
        }

        @Monitor(name = "share_in_bytes", type = DataSourceType.GAUGE)
        public long getShareInBytes() {
            return processStatsBean.get().shareInBytes;
        }

        @Monitor(name = "total_virtual_in_bytes", type = DataSourceType.GAUGE)
        public long getTotalVirtualInBytes() {
            return processStatsBean.get().totalVirtualInBytes;
        }

        @Monitor(name = "cpu_percent", type = DataSourceType.GAUGE)
        public short getCpuPercent() {
            return processStatsBean.get().cpuPercent;
        }

        @Monitor(name = "sys_in_millis", type = DataSourceType.GAUGE)
        public long getSysInMillis() {
            return processStatsBean.get().sysInMillis;
        }

        @Monitor(name = "user_in_millis", type = DataSourceType.GAUGE)
        public long getUserInMillis() {
            return processStatsBean.get().userInMillis;
        }

        @Monitor(name = "total_in_millis", type = DataSourceType.GAUGE)
        public long getTotalInMillis() {
            return processStatsBean.get().totalInMillis;
        }

        @Monitor(name = "open_file_descriptors", type = DataSourceType.GAUGE)
        public double getOpenFileDescriptors() {
            return processStatsBean.get().openFileDescriptors;
        }

        @Monitor(name = "cpu_timestamp", type = DataSourceType.GAUGE)
        public long getCpuTimestamp() {
            return processStatsBean.get().cpuTimestamp;
        }
    }

    private static class ProcessStatsBean {
        private long residentInBytes;
        private long shareInBytes;
        private long totalVirtualInBytes;
        private short cpuPercent;
        private long sysInMillis;
        private long userInMillis;
        private long totalInMillis;
        private long openFileDescriptors;
        private long cpuTimestamp;
    }

    public static TaskTimer getTimer(String name) {
        return new SimpleTimer(name, 60 * 1000);
    }

    @Override
    public String getName() {
        return METRIC_NAME;
    }
}
