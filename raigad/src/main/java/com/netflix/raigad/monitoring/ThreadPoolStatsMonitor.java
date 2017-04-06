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
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class ThreadPoolStatsMonitor extends Task {
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_ThreadPoolMonitor";
    private final Elasticsearch_ThreadPoolStatsReporter tpStatsReporter;

    @Inject
    public ThreadPoolStatsMonitor(IConfiguration config) {
        super(config);
        tpStatsReporter = new Elasticsearch_ThreadPoolStatsReporter();
        Monitors.registerObject(tpStatsReporter);
    }

    @Override
    public void execute() throws Exception {
        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        ThreadPoolStatsBean threadPoolStatsBean = new ThreadPoolStatsBean();

        try {
            NodesStatsResponse nodesStatsResponse = ElasticsearchTransportClient.getNodesStatsResponse(config);
            NodeStats nodeStats = null;

            List<NodeStats> nodeStatsList = nodesStatsResponse.getNodes();

            if (nodeStatsList.size() > 0) {
                nodeStats = nodeStatsList.get(0);
            }

            if (nodeStats == null) {
                logger.info("Thread pool stats are not available (node stats is not available)");
                return;
            }

            ThreadPoolStats threadPoolStats = nodeStats.getThreadPool();

            if (threadPoolStats == null) {
                logger.info("Thread pool stats are not available");
                return;
            }

            Iterator<ThreadPoolStats.Stats> threadPoolStatsIterator = threadPoolStats.iterator();

            while (threadPoolStatsIterator.hasNext()) {
                ThreadPoolStats.Stats stat = threadPoolStatsIterator.next();
                if (stat.getName().equals("index")) {
                    threadPoolStatsBean.indexThreads = stat.getThreads();
                    threadPoolStatsBean.indexQueue = stat.getQueue();
                    threadPoolStatsBean.indexActive = stat.getActive();
                    threadPoolStatsBean.indexRejected = stat.getRejected();
                    threadPoolStatsBean.indexLargest = stat.getLargest();
                    threadPoolStatsBean.indexCompleted = stat.getCompleted();
                } else if (stat.getName().equals("get")) {
                    threadPoolStatsBean.getThreads = stat.getThreads();
                    threadPoolStatsBean.getQueue = stat.getQueue();
                    threadPoolStatsBean.getActive = stat.getActive();
                    threadPoolStatsBean.getRejected = stat.getRejected();
                    threadPoolStatsBean.getLargest = stat.getLargest();
                    threadPoolStatsBean.getCompleted = stat.getCompleted();
                } else if (stat.getName().equals("search")) {
                    threadPoolStatsBean.searchThreads = stat.getThreads();
                    threadPoolStatsBean.searchQueue = stat.getQueue();
                    threadPoolStatsBean.searchActive = stat.getActive();
                    threadPoolStatsBean.searchRejected = stat.getRejected();
                    threadPoolStatsBean.searchLargest = stat.getLargest();
                    threadPoolStatsBean.searchCompleted = stat.getCompleted();
                } else if (stat.getName().equals("bulk")) {
                    threadPoolStatsBean.bulkThreads = stat.getThreads();
                    threadPoolStatsBean.bulkQueue = stat.getQueue();
                    threadPoolStatsBean.bulkActive = stat.getActive();
                    threadPoolStatsBean.bulkRejected = stat.getRejected();
                    threadPoolStatsBean.bulkLargest = stat.getLargest();
                    threadPoolStatsBean.bulkCompleted = stat.getCompleted();
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to load thread pool stats data", e);
        }

        tpStatsReporter.threadPoolBean.set(threadPoolStatsBean);
    }

    public class Elasticsearch_ThreadPoolStatsReporter {
        private final AtomicReference<ThreadPoolStatsBean> threadPoolBean;

        public Elasticsearch_ThreadPoolStatsReporter() {
            threadPoolBean = new AtomicReference<ThreadPoolStatsBean>(new ThreadPoolStatsBean());
        }

        @Monitor(name = "IndexThreads", type = DataSourceType.GAUGE)
        public long getIndexThreads() {
            return threadPoolBean.get().indexThreads;
        }

        @Monitor(name = "IndexQueue", type = DataSourceType.GAUGE)
        public long getIndexQueue() {
            return threadPoolBean.get().indexQueue;
        }

        @Monitor(name = "indexActive", type = DataSourceType.GAUGE)
        public long getIndexActive() {
            return threadPoolBean.get().indexActive;
        }

        @Monitor(name = "indexRejected", type = DataSourceType.COUNTER)
        public long getIndexRejected() {
            return threadPoolBean.get().indexRejected;
        }

        @Monitor(name = "indexLargest", type = DataSourceType.GAUGE)
        public long getIndexLargest() {
            return threadPoolBean.get().indexLargest;
        }

        @Monitor(name = "indexCompleted", type = DataSourceType.COUNTER)
        public long getIndexCompleted() {
            return threadPoolBean.get().indexCompleted;
        }

        @Monitor(name = "getThreads", type = DataSourceType.GAUGE)
        public long getGetThreads() {
            return threadPoolBean.get().getThreads;
        }

        @Monitor(name = "getQueue", type = DataSourceType.GAUGE)
        public long getGetQueue() {
            return threadPoolBean.get().getQueue;
        }

        @Monitor(name = "getActive", type = DataSourceType.GAUGE)
        public long getGetActive() {
            return threadPoolBean.get().getActive;
        }

        @Monitor(name = "getRejected", type = DataSourceType.COUNTER)
        public long getGetRejected() {
            return threadPoolBean.get().getRejected;
        }

        @Monitor(name = "getLargest", type = DataSourceType.GAUGE)
        public long getGetLargest() {
            return threadPoolBean.get().getLargest;
        }

        @Monitor(name = "getCompleted", type = DataSourceType.COUNTER)
        public long getGetCompleted() {
            return threadPoolBean.get().getCompleted;
        }

        @Monitor(name = "searchThreads", type = DataSourceType.GAUGE)
        public long getSearchThreads() {
            return threadPoolBean.get().searchThreads;
        }

        @Monitor(name = "searchQueue", type = DataSourceType.GAUGE)
        public long getSearchQueue() {
            return threadPoolBean.get().searchQueue;
        }

        @Monitor(name = "searchActive", type = DataSourceType.GAUGE)
        public long getSearchActive() {
            return threadPoolBean.get().searchActive;
        }

        @Monitor(name = "searchRejected", type = DataSourceType.COUNTER)
        public long getSearchRejected() {
            return threadPoolBean.get().searchRejected;
        }

        @Monitor(name = "searchLargest", type = DataSourceType.GAUGE)
        public long getSearchLargest() {
            return threadPoolBean.get().searchLargest;
        }

        @Monitor(name = "searchCompleted", type = DataSourceType.COUNTER)
        public long getSearchCompleted() {
            return threadPoolBean.get().searchCompleted;
        }

        @Monitor(name = "bulkThreads", type = DataSourceType.GAUGE)
        public long getBulkThreads() {
            return threadPoolBean.get().bulkThreads;
        }

        @Monitor(name = "bulkQueue", type = DataSourceType.GAUGE)
        public long getBulkQueue() {
            return threadPoolBean.get().bulkQueue;
        }

        @Monitor(name = "bulkActive", type = DataSourceType.GAUGE)
        public long getBulkActive() {
            return threadPoolBean.get().bulkActive;
        }

        @Monitor(name = "bulkRejected", type = DataSourceType.COUNTER)
        public long getBulkRejected() {
            return threadPoolBean.get().bulkRejected;
        }

        @Monitor(name = "bulkLargest", type = DataSourceType.GAUGE)
        public long getBulkLargest() {
            return threadPoolBean.get().bulkLargest;
        }

        @Monitor(name = "bulkCompleted", type = DataSourceType.COUNTER)
        public long getBulkCompleted() {
            return threadPoolBean.get().bulkCompleted;
        }
    }

    private static class ThreadPoolStatsBean {
        private long indexThreads;
        private long indexQueue;
        private long indexActive;
        private long indexRejected;
        private long indexLargest;
        private long indexCompleted;

        private long getThreads;
        private long getQueue;
        private long getActive;
        private long getRejected;
        private long getLargest;
        private long getCompleted;

        private long searchThreads;
        private long searchQueue;
        private long searchActive;
        private long searchRejected;
        private long searchLargest;
        private long searchCompleted;

        private long bulkThreads;
        private long bulkQueue;
        private long bulkActive;
        private long bulkRejected;
        private long bulkLargest;
        private long bulkCompleted;
    }

    public static TaskTimer getTimer(String name) {
        return new SimpleTimer(name, 60 * 1000);
    }

    @Override
    public String getName() {
        return METRIC_NAME;
    }

}
