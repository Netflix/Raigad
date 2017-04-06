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
import org.elasticsearch.http.HttpStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class HttpStatsMonitor extends Task {
    private static final Logger logger = LoggerFactory.getLogger(HttpStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_HttpStatsMonitor";
    private final Elasticsearch_HttpStatsReporter httpStatsReporter;

    @Inject
    public HttpStatsMonitor(IConfiguration config) {
        super(config);
        httpStatsReporter = new Elasticsearch_HttpStatsReporter();
        Monitors.registerObject(httpStatsReporter);
    }

    @Override
    public void execute() throws Exception {
        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        HttpStatsBean httpStatsBean = new HttpStatsBean();

        try {
            NodesStatsResponse nodesStatsResponse = ElasticsearchTransportClient.getNodesStatsResponse(config);
            NodeStats nodeStats = null;

            List<NodeStats> nodeStatsList = nodesStatsResponse.getNodes();

            if (nodeStatsList.size() > 0) {
                nodeStats = nodeStatsList.get(0);
            }

            if (nodeStats == null) {
                logger.info("HTTP stats is not available (node stats are not available)");
                return;
            }

            HttpStats httpStats = nodeStats.getHttp();

            if (httpStats == null) {
                logger.info("HTTP stats is not available");
                return;
            }

            httpStatsBean.serverOpen = httpStats.getServerOpen();
            httpStatsBean.totalOpen = httpStats.getTotalOpen();
        } catch (Exception e) {
            logger.warn("failed to load HTTP stats data", e);
        }

        httpStatsReporter.httpStatsBean.set(httpStatsBean);
    }

    public class Elasticsearch_HttpStatsReporter {
        private final AtomicReference<HttpStatsBean> httpStatsBean;

        public Elasticsearch_HttpStatsReporter() {
            httpStatsBean = new AtomicReference<>(new HttpStatsBean());
        }

        @Monitor(name = "server_open", type = DataSourceType.GAUGE)
        public long getServerOpen() {
            return httpStatsBean.get().serverOpen;
        }

        @Monitor(name = "total_open", type = DataSourceType.GAUGE)
        public long getTotalOpen() {
            return httpStatsBean.get().totalOpen;
        }
    }

    private static class HttpStatsBean {
        private long serverOpen;
        private long totalOpen;
    }

    public static TaskTimer getTimer(String name) {
        return new SimpleTimer(name, 60 * 1000);
    }

    @Override
    public String getName() {
        return METRIC_NAME;
    }

}
