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
import org.elasticsearch.monitor.jvm.JvmStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class JvmStatsMonitor extends Task {
    private static final Logger logger = LoggerFactory.getLogger(JvmStatsMonitor.class);

    public static final String METRIC_NAME = "Elasticsearch_JvmStatsMonitor";
    public static final String GC_YOUNG_TAG = "young";
    public static final String GC_OLD_TAG = "old";
    public static final String GC_SURVIVOR_TAG = "survivor";

    private final Elasticsearch_JvmStatsReporter jvmStatsReporter;

    @Inject
    public JvmStatsMonitor(IConfiguration config) {
        super(config);
        jvmStatsReporter = new Elasticsearch_JvmStatsReporter();
        Monitors.registerObject(jvmStatsReporter);
    }

    @Override
    public void execute() throws Exception {
        // Only start monitoring if Elasticsearch is started
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        JvmStatsBean jvmStatsBean = new JvmStatsBean();

        try {
            NodesStatsResponse nodesStatsResponse = ESTransportClient.getNodesStatsResponse(config);
            JvmStats jvmStats;
            NodeStats nodeStats = null;

            if (nodesStatsResponse.getNodes().length > 0) {
                nodeStats = nodesStatsResponse.getAt(0);
            }

            if (nodeStats == null) {
                logger.info("JVM stats is not available (node stats is not available)");
                return;
            }

            jvmStats = nodeStats.getJvm();
            if (jvmStats == null) {
                logger.info("JVM stats is not available");
                return;
            }

            //Heap
            jvmStatsBean.heapCommittedInBytes = jvmStats.getMem().getHeapCommitted().getMb();
            jvmStatsBean.heapMaxInBytes = jvmStats.getMem().getHeapMax().getMb();
            jvmStatsBean.heapUsedInBytes = jvmStats.getMem().getHeapUsed().getMb();
            jvmStatsBean.heapUsedPercent = jvmStats.getMem().getHeapUsedPercent();
            jvmStatsBean.nonHeapCommittedInBytes = jvmStats.getMem().getNonHeapCommitted().getMb();
            jvmStatsBean.nonHeapUsedInBytes = jvmStats.getMem().getNonHeapUsed().getMb();

            Iterator<JvmStats.MemoryPool> memoryPoolIterator = jvmStats.getMem().iterator();

            while (memoryPoolIterator.hasNext()) {
                JvmStats.MemoryPool memoryPoolStats = memoryPoolIterator.next();
                if (memoryPoolStats.getName().equalsIgnoreCase(GC_YOUNG_TAG)) {
                    jvmStatsBean.youngMaxInBytes = memoryPoolStats.getMax().getBytes();
                    jvmStatsBean.youngUsedInBytes = memoryPoolStats.getUsed().getBytes();
                    jvmStatsBean.youngPeakUsedInBytes = memoryPoolStats.getPeakUsed().getBytes();
                    jvmStatsBean.youngPeakMaxInBytes = memoryPoolStats.getPeakMax().getBytes();
                }
                else if (memoryPoolStats.getName().equalsIgnoreCase(GC_SURVIVOR_TAG)) {
                    jvmStatsBean.survivorMaxInBytes = memoryPoolStats.getMax().getBytes();
                    jvmStatsBean.survivorUsedInBytes = memoryPoolStats.getUsed().getBytes();
                    jvmStatsBean.survivorPeakUsedInBytes = memoryPoolStats.getPeakUsed().getBytes();
                    jvmStatsBean.survivorPeakMaxInBytes = memoryPoolStats.getPeakMax().getBytes();
                }
                else if (memoryPoolStats.getName().equalsIgnoreCase(GC_OLD_TAG)) {
                    jvmStatsBean.oldMaxInBytes = memoryPoolStats.getMax().getBytes();
                    jvmStatsBean.oldUsedInBytes = memoryPoolStats.getUsed().getBytes();
                    jvmStatsBean.oldPeakUsedInBytes = memoryPoolStats.getPeakUsed().getBytes();
                    jvmStatsBean.oldPeakMaxInBytes = memoryPoolStats.getPeakMax().getBytes();
                }
            }

            //Threads
            jvmStatsBean.threadCount = jvmStats.getThreads().getCount();
            jvmStatsBean.threadPeakCount = jvmStats.getThreads().getPeakCount();
            jvmStatsBean.uptimeHours = jvmStats.getUptime().getHours();

            //GC
            for (JvmStats.GarbageCollector garbageCollector : jvmStats.getGc().getCollectors()) {
                if (garbageCollector.getName().equalsIgnoreCase(GC_YOUNG_TAG)) {
                    jvmStatsBean.youngCollectionCount = garbageCollector.getCollectionCount();
                    jvmStatsBean.youngCollectionTimeInMillis = garbageCollector.getCollectionTime().getMillis();

                    /* TODO: 2X: Determine if last GC is necessary and if yes find an alternative
                    if (garbageCollector.getLastGc() != null) {
                        jvmStatsBean.youngLastGcStartTime = garbageCollector.getLastGc().getStartTime();
                        jvmStatsBean.youngLastGcEndTime = garbageCollector.getLastGc().getEndTime();
                        jvmStatsBean.youngLastGcDuration = garbageCollector.getLastGc().getDuration().getMillis();
                        jvmStatsBean.youngLastGcMaxInBytes = garbageCollector.getLastGc().getMax().getBytes();
                        jvmStatsBean.youngLastGcBeforeUsedInBytes = garbageCollector.getLastGc().getBeforeUsed().getBytes();
                        jvmStatsBean.youngLastGcAfterUsedInBytes = garbageCollector.getLastGc().getAfterUsed().getBytes();
                    }
                    */
                } else if (garbageCollector.getName().equalsIgnoreCase(GC_OLD_TAG)) {
                    jvmStatsBean.oldCollectionCount = garbageCollector.getCollectionCount();
                    jvmStatsBean.oldCollectionTimeInMillis = garbageCollector.getCollectionTime().getMillis();

                    /* TODO: 2X: Determine if last GC is necessary and if yes find an alternative
                    if (garbageCollector.getLastGc() != null) {
                        jvmStatsBean.oldLastGcStartTime = garbageCollector.getLastGc().getStartTime();
                        jvmStatsBean.oldLastGcEndTime = garbageCollector.getLastGc().getEndTime();
                        jvmStatsBean.oldLastGcDuration = garbageCollector.getLastGc().getDuration().getMillis();
                        jvmStatsBean.oldLastGcMaxInBytes = garbageCollector.getLastGc().getMax().getBytes();
                        jvmStatsBean.oldLastGcBeforeUsedInBytes = garbageCollector.getLastGc().getBeforeUsed().getBytes();
                        jvmStatsBean.oldLastGcAfterUsedInBytes = garbageCollector.getLastGc().getAfterUsed().getBytes();
                    }
                    */
                }
            }
        }
        catch (Exception e) {
            logger.warn("Failed to load JVM stats data", e);
        }

        jvmStatsReporter.jvmStatsBean.set(jvmStatsBean);
    }

    public class Elasticsearch_JvmStatsReporter {
        private final AtomicReference<JvmStatsBean> jvmStatsBean;

        public Elasticsearch_JvmStatsReporter() {
            jvmStatsBean = new AtomicReference<JvmStatsBean>(new JvmStatsBean());
        }

        @Monitor(name = "heap_committed_in_bytes", type = DataSourceType.GAUGE)
        public long getHeapCommitedInBytes() {
            return jvmStatsBean.get().heapCommittedInBytes;
        }

        @Monitor(name = "heap_max_in_bytes", type = DataSourceType.GAUGE)
        public long getHeapMaxInBytes() {
            return jvmStatsBean.get().heapMaxInBytes;
        }

        @Monitor(name = "heap_used_in_bytes", type = DataSourceType.GAUGE)
        public long getHeapUsedInBytes() {
            return jvmStatsBean.get().heapUsedInBytes;
        }

        @Monitor(name = "non_heap_committed_in_bytes", type = DataSourceType.GAUGE)
        public long getNonHeapCommittedInBytes() {
            return jvmStatsBean.get().nonHeapCommittedInBytes;
        }

        @Monitor(name = "non_heap_used_in_bytes", type = DataSourceType.GAUGE)
        public long getNonHeapUsedInBytes() {
            return jvmStatsBean.get().nonHeapUsedInBytes;
        }

        @Monitor(name = "heap_used_percent", type = DataSourceType.GAUGE)
        public short getHeapUsedPercent() {
            return jvmStatsBean.get().heapUsedPercent;
        }

        @Monitor(name = "threads_count", type = DataSourceType.GAUGE)
        public long getThreadsCount() {
            return jvmStatsBean.get().threadCount;
        }

        @Monitor(name = "threads_peak_count", type = DataSourceType.GAUGE)
        public long getThreadsPeakCount() {
            return jvmStatsBean.get().threadPeakCount;
        }

        @Monitor(name = "uptime_hours", type = DataSourceType.GAUGE)
        public double getUptimeHours() {
            return jvmStatsBean.get().uptimeHours;
        }

        @Monitor(name = "young_collection_count", type = DataSourceType.GAUGE)
        public long getYoungCollectionCount() {
            return jvmStatsBean.get().youngCollectionCount;
        }

        @Monitor(name = "young_collection_time_in_millis", type = DataSourceType.GAUGE)
        public long getYoungCollectionTimeInMillis() {
            return jvmStatsBean.get().youngCollectionTimeInMillis;
        }

        @Monitor(name = "old_collection_count", type = DataSourceType.GAUGE)
        public long getOldCollectionCount() {
            return jvmStatsBean.get().oldCollectionCount;
        }

        @Monitor(name = "old_collection_time_in_millis", type = DataSourceType.GAUGE)
        public long getOldCollectionTimeInMillis() {
            return jvmStatsBean.get().oldCollectionTimeInMillis;
        }

        @Monitor(name = "young_used_in_bytes", type = DataSourceType.GAUGE)
        public long getYoungUsedInBytes() {
            return jvmStatsBean.get().youngUsedInBytes;
        }

        @Monitor(name = "young_max_in_bytes", type = DataSourceType.GAUGE)
        public long getYoungMaxInBytes() {
            return jvmStatsBean.get().youngMaxInBytes;
        }

        @Monitor(name = "young_peak_used_in_bytes", type = DataSourceType.GAUGE)
        public long getYoungPeakUsedInBytes() {
            return jvmStatsBean.get().youngPeakUsedInBytes;
        }

        @Monitor(name = "young_peak_max_in_bytes", type = DataSourceType.GAUGE)
        public long getYoungPeakMaxInBytes() {
            return jvmStatsBean.get().youngPeakMaxInBytes;
        }

        @Monitor(name = "survivor_used_in_bytes", type = DataSourceType.GAUGE)
        public long getSurvivorUsedInBytes() {
            return jvmStatsBean.get().survivorUsedInBytes;
        }

        @Monitor(name = "survivor_max_in_bytes", type = DataSourceType.GAUGE)
        public long getSurvivorMaxInBytes() {
            return jvmStatsBean.get().survivorMaxInBytes;
        }

        @Monitor(name = "survivor_peak_used_in_bytes", type = DataSourceType.GAUGE)
        public long getSurvivorPeakUsedInBytes() {
            return jvmStatsBean.get().survivorPeakUsedInBytes;
        }

        @Monitor(name = "survivor_peak_max_in_bytes", type = DataSourceType.GAUGE)
        public long getSurvivorPeakMaxInBytes() {
            return jvmStatsBean.get().survivorPeakMaxInBytes;
        }

        @Monitor(name = "old_used_in_bytes", type = DataSourceType.GAUGE)
        public long getOldUsedInBytes() {
            return jvmStatsBean.get().oldUsedInBytes;
        }

        @Monitor(name = "old_max_in_bytes", type = DataSourceType.GAUGE)
        public long getOldMaxInBytes() {
            return jvmStatsBean.get().oldMaxInBytes;
        }

        @Monitor(name = "old_peak_used_in_bytes", type = DataSourceType.GAUGE)
        public long getOldPeakUsedInBytes() {
            return jvmStatsBean.get().oldPeakUsedInBytes;
        }

        @Monitor(name = "old_peak_max_in_bytes", type = DataSourceType.GAUGE)
        public long getOldPeakMaxInBytes() {
            return jvmStatsBean.get().oldPeakMaxInBytes;
        }

        @Monitor(name = "young_last_gc_start_time", type = DataSourceType.GAUGE)
        public long getYoungLastGcStartTime() {
            return jvmStatsBean.get().youngLastGcStartTime;
        }

        @Monitor(name = "young_last_gc_end_time", type = DataSourceType.GAUGE)
        public long getYoungLastGcEndTime() {
            return jvmStatsBean.get().youngLastGcEndTime;
        }

        @Monitor(name = "young_last_gc_max_in_bytes", type = DataSourceType.GAUGE)
        public long getYoungLastGcMaxInBytes() {
            return jvmStatsBean.get().youngLastGcMaxInBytes;
        }

        @Monitor(name = "young_last_gc_before_used_in_bytes", type = DataSourceType.GAUGE)
        public long getYoungLastGcBeforeUsedInBytes() {
            return jvmStatsBean.get().youngLastGcBeforeUsedInBytes;
        }

        @Monitor(name = "young_last_gc_after_used_in_bytes", type = DataSourceType.GAUGE)
        public long getYoungLastGcAfterUsedInBytes() {
            return jvmStatsBean.get().youngLastGcAfterUsedInBytes;
        }

        @Monitor(name = "young_last_gc_duration", type = DataSourceType.GAUGE)
        public long getYoungLastGcDuration() {
            return jvmStatsBean.get().youngLastGcDuration;
        }

        @Monitor(name = "old_last_gc_start_time", type = DataSourceType.GAUGE)
        public long getOldLastGcStartTime() {
            return jvmStatsBean.get().oldLastGcStartTime;
        }

        @Monitor(name = "old_last_gc_end_time", type = DataSourceType.GAUGE)
        public long getOldLastGcEndTime() {
            return jvmStatsBean.get().oldLastGcEndTime;
        }

        @Monitor(name = "old_last_gc_max_in_bytes", type = DataSourceType.GAUGE)
        public long getOldLastGcMaxInBytes() {
            return jvmStatsBean.get().oldLastGcMaxInBytes;
        }

        @Monitor(name = "old_last_gc_before_used_in_bytes", type = DataSourceType.GAUGE)
        public long getOldLastGcBeforeUsedInBytes() {
            return jvmStatsBean.get().oldLastGcBeforeUsedInBytes;
        }

        @Monitor(name = "old_last_gc_after_used_in_bytes", type = DataSourceType.GAUGE)
        public long getOldLastGcAfterUsedInBytes() {
            return jvmStatsBean.get().oldLastGcAfterUsedInBytes;
        }

        @Monitor(name = "old_last_gc_duration", type = DataSourceType.GAUGE)
        public long getOldLastGcDuration() {
            return jvmStatsBean.get().oldLastGcDuration;
        }
    }

    private static class JvmStatsBean {
        private long heapCommittedInBytes;
        private long heapMaxInBytes;
        private long heapUsedInBytes;
        private long nonHeapCommittedInBytes;
        private long nonHeapUsedInBytes;
        private short heapUsedPercent;
        private int threadCount;
        private int threadPeakCount;
        private long uptimeHours;
        private long youngCollectionCount;
        private long youngCollectionTimeInMillis;
        private long oldCollectionCount;
        private long oldCollectionTimeInMillis;
        private long youngUsedInBytes;
        private long youngMaxInBytes;
        private long youngPeakUsedInBytes;
        private long youngPeakMaxInBytes;
        private long survivorUsedInBytes;
        private long survivorMaxInBytes;
        private long survivorPeakUsedInBytes;
        private long survivorPeakMaxInBytes;
        private long oldUsedInBytes;
        private long oldMaxInBytes;
        private long oldPeakUsedInBytes;
        private long oldPeakMaxInBytes;
        private long youngLastGcStartTime;
        private long youngLastGcEndTime;
        private long youngLastGcMaxInBytes;
        private long youngLastGcBeforeUsedInBytes;
        private long youngLastGcAfterUsedInBytes;
        private long youngLastGcDuration;
        private long oldLastGcStartTime;
        private long oldLastGcEndTime;
        private long oldLastGcMaxInBytes;
        private long oldLastGcBeforeUsedInBytes;
        private long oldLastGcAfterUsedInBytes;
        private long oldLastGcDuration;
    }

    public static TaskTimer getTimer(String name) {
        return new SimpleTimer(name, 60 * 1000);
    }

    @Override
    public String getName() {
        return METRIC_NAME;
    }

}
