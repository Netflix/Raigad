/**
 * Copyright 2017 Netflix, Inc.
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
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import com.netflix.raigad.utils.ElasticsearchTransportClient;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;


@Singleton
public class AllCircuitBreakerStatsMonitor extends Task {
    private static final Logger logger = LoggerFactory.getLogger(AllCircuitBreakerStatsMonitor.class);

    public static final String METRIC_NAME = "Elasticsearch_AllCircuitBreakerStatsMonitor";
    private final Elasticsearch_AllCircuitBreakerStatsReporter allCircuitBreakerStatsReporter;

    @Inject
    public AllCircuitBreakerStatsMonitor(IConfiguration config) {
        super(config);
        allCircuitBreakerStatsReporter = new Elasticsearch_AllCircuitBreakerStatsReporter();
        Monitors.registerObject(allCircuitBreakerStatsReporter);
    }

    @Override
    public void execute() throws Exception {
        // Only start monitoring if Elasticsearch is started
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        AllCircuitBreakerStatsBean allCircuitBreakerStatsBean = new AllCircuitBreakerStatsBean();

        try {
            NodesStatsResponse nodesStatsResponse = ElasticsearchTransportClient.getNodesStatsResponse(config);
            NodeStats nodeStats = null;

            if (nodesStatsResponse.getNodes().length > 0) {
                nodeStats = nodesStatsResponse.getAt(0);
            }

            if (nodeStats == null) {
                logger.info("Circuit breaker stats is not available (node stats is not available)");
                return;
            }

            AllCircuitBreakerStats allCircuitBreakerStats = nodeStats.getBreaker();
            if (allCircuitBreakerStats == null) {
                logger.info("Circuit breaker stats is not available");
                return;
            }

            CircuitBreakerStats[] circuitBreakerStats = allCircuitBreakerStats.getAllStats();
            if (circuitBreakerStats == null || circuitBreakerStats.length == 0) {
                logger.info("Circuit breaker stats is not available (stats are empty)");
                return;
            }

            for (CircuitBreakerStats circuitBreakerStat : circuitBreakerStats) {
                if (circuitBreakerStat.getName() == CircuitBreaker.FIELDDATA) {
                    allCircuitBreakerStatsBean.fieldDataEstimatedSizeInBytes = circuitBreakerStat.getEstimated();
                    allCircuitBreakerStatsBean.fieldDataLimitMaximumSizeInBytes = circuitBreakerStat.getLimit();
                    allCircuitBreakerStatsBean.fieldDataOverhead = circuitBreakerStat.getOverhead();
                    allCircuitBreakerStatsBean.fieldDataTrippedCount = circuitBreakerStat.getTrippedCount();
                }

                if (circuitBreakerStat.getName() == CircuitBreaker.REQUEST) {
                    allCircuitBreakerStatsBean.requestEstimatedSizeInBytes = circuitBreakerStat.getEstimated();
                    allCircuitBreakerStatsBean.requestLimitMaximumSizeInBytes = circuitBreakerStat.getLimit();
                    allCircuitBreakerStatsBean.requestOverhead = circuitBreakerStat.getOverhead();
                    allCircuitBreakerStatsBean.requestTrippedCount = circuitBreakerStat.getTrippedCount();
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to load circuit breaker stats data", e);
        }

        allCircuitBreakerStatsReporter.allCircuitBreakerStatsBean.set(allCircuitBreakerStatsBean);
    }

    public class Elasticsearch_AllCircuitBreakerStatsReporter {
        private final AtomicReference<AllCircuitBreakerStatsBean> allCircuitBreakerStatsBean;

        public Elasticsearch_AllCircuitBreakerStatsReporter() {
            allCircuitBreakerStatsBean = new AtomicReference<AllCircuitBreakerStatsBean>(new AllCircuitBreakerStatsBean());
        }

        @Monitor(name = "field_data_estimated_size_in_bytes", type = DataSourceType.GAUGE)
        public long getFieldDataEstimatedSizeInBytes() {
            return allCircuitBreakerStatsBean.get().fieldDataEstimatedSizeInBytes;
        }

        @Monitor(name = "field_data_limit_maximum_size_in_bytes", type = DataSourceType.GAUGE)
        public long getFieldDataLimitMaximumSizeInBytes() {
            return allCircuitBreakerStatsBean.get().fieldDataLimitMaximumSizeInBytes;
        }

        @Monitor(name = "field_data_tripped_count", type = DataSourceType.GAUGE)
        public double getFieldDataTrippedCount() {
            return allCircuitBreakerStatsBean.get().fieldDataTrippedCount;
        }

        @Monitor(name = "field_data_overhead", type = DataSourceType.GAUGE)
        public double getFieldDataOverhead() {
            return allCircuitBreakerStatsBean.get().fieldDataOverhead;
        }

        @Monitor(name = "request_estimated_size_in_bytes", type = DataSourceType.GAUGE)
        public long getRequestEstimatedSizeInBytes() {
            return allCircuitBreakerStatsBean.get().requestEstimatedSizeInBytes;
        }

        @Monitor(name = "request_limit_maximum_size_in_bytes", type = DataSourceType.GAUGE)
        public long getRequestLimitMaximumSizeInBytes() {
            return allCircuitBreakerStatsBean.get().requestLimitMaximumSizeInBytes;
        }

        @Monitor(name = "request_tripped_count", type = DataSourceType.GAUGE)
        public double getRequestTrippedCount() {
            return allCircuitBreakerStatsBean.get().requestTrippedCount;
        }

        @Monitor(name = "request_overhead", type = DataSourceType.GAUGE)
        public double getRequestOverhead() {
            return allCircuitBreakerStatsBean.get().requestOverhead;
        }
    }

    private static class AllCircuitBreakerStatsBean {
        private long fieldDataEstimatedSizeInBytes;
        private long fieldDataLimitMaximumSizeInBytes;
        private long fieldDataTrippedCount;
        private double fieldDataOverhead;
        private long requestEstimatedSizeInBytes;
        private long requestLimitMaximumSizeInBytes;
        private long requestTrippedCount;
        private double requestOverhead;
    }

    public static TaskTimer getTimer(String name) {
        return new SimpleTimer(name, 60 * 1000);
    }

    @Override
    public String getName() {
        return METRIC_NAME;
    }
}
