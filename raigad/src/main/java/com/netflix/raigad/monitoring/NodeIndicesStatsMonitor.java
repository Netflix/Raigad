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
import org.elasticsearch.indices.NodeIndicesStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Note regarding - Percentiles over Average Latencies
 *
 * Currently ES provides only cumulative query & index time along with cumulative query & index count.
 * Hence Percentile values are calculated based on the average between consecutive time
 * (t1 & t2, t2 & t3, ... , tn-1 & tn) of metrics collection.
 *
 */
@Singleton
public class NodeIndicesStatsMonitor extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(NodeIndicesStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_NodeIndicesMonitor";
    private final Elasticsearch_NodeIndicesStatsReporter nodeIndicesStatsReporter;
    private final EstimatedHistogram latencySearchQuery95Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencySearchQuery99Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencySearchFetch95Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencySearchFetch99Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyGet95Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyGet99Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyGetExists95Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyGetExists99Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyGetMissing95Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyGetMissing99Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyIndexing95Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyIndexing99Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyIndexDelete95Histo = new EstimatedHistogram();
    private final EstimatedHistogram latencyIndexDelete99Histo = new EstimatedHistogram();
    private final double PERCENTILE_95 = 0.95;
    private final double PERCENTILE_99 = 0.99;

    @Inject
    public NodeIndicesStatsMonitor(IConfiguration config)
    {
        super(config);
        nodeIndicesStatsReporter = new Elasticsearch_NodeIndicesStatsReporter();
        Monitors.registerObject(nodeIndicesStatsReporter);
    }

    @Override
    public void execute() throws Exception {

        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        NodeIndicesStatsBean nodeIndicesStatsBean = new NodeIndicesStatsBean();
        try
        {
            NodesStatsResponse ndsStatsResponse = ESTransportClient.getNodesStatsResponse(config);
            NodeIndicesStats nodeIndicesStats = null;
            NodeStats ndStat = null;
            if (ndsStatsResponse.getNodes().length > 0) {
                ndStat = ndsStatsResponse.getAt(0);
            }
            if (ndStat == null) {
                logger.info("NodeIndicesStats is null,hence returning (No NodeIndicesStats).");
                resetNodeIndicesStats(nodeIndicesStatsBean);
                return;
            }
            nodeIndicesStats = ndStat.getIndices();
            if (nodeIndicesStats == null) {
                logger.info("NodeIndicesStats is null,hence returning (No NodeIndicesStats).");
                resetNodeIndicesStats(nodeIndicesStatsBean);
                return;
            }

            updateStoreDocs(nodeIndicesStatsBean,nodeIndicesStats);

            updateRefreshFlush(nodeIndicesStatsBean,nodeIndicesStats);

            updateMerge(nodeIndicesStatsBean,nodeIndicesStats);

            updateCache(nodeIndicesStatsBean,nodeIndicesStats);

            updateSearch(nodeIndicesStatsBean,nodeIndicesStats);

            updateGet(nodeIndicesStatsBean,nodeIndicesStats);

            updateIndexing(nodeIndicesStatsBean,nodeIndicesStats);

        }
        catch(Exception e)
        {
            resetNodeIndicesStats(nodeIndicesStatsBean);
            logger.warn("failed to load Indices stats data", e);
        }

        nodeIndicesStatsReporter.nodeIndicesStatsBean.set(nodeIndicesStatsBean);
    }

    private void updateStoreDocs(NodeIndicesStatsBean nodeIndicesStatsBean, NodeIndicesStats nodeIndicesStats)
    {
        nodeIndicesStatsBean.storeSize = nodeIndicesStats.getStore().getSizeInBytes();
        nodeIndicesStatsBean.storeThrottleTime = nodeIndicesStats.getStore().getThrottleTime().millis();
        nodeIndicesStatsBean.docsCount = nodeIndicesStats.getDocs().getCount();
        nodeIndicesStatsBean.docsDeleted = nodeIndicesStats.getDocs().getDeleted();
    }

    private void updateRefreshFlush(NodeIndicesStatsBean nodeIndicesStatsBean, NodeIndicesStats nodeIndicesStats)
    {
        nodeIndicesStatsBean.refreshTotal = nodeIndicesStats.getRefresh().getTotal();
        nodeIndicesStatsBean.refreshTotalTime = nodeIndicesStats.getRefresh().getTotalTimeInMillis();
        if (nodeIndicesStatsBean.refreshTotal != 0)
            nodeIndicesStatsBean.refreshAvgTimeInMillisPerRequest = nodeIndicesStatsBean.refreshTotalTime / nodeIndicesStatsBean.refreshTotal;

        nodeIndicesStatsBean.flushTotal = nodeIndicesStats.getFlush().getTotal();
        nodeIndicesStatsBean.flushTotalTime = nodeIndicesStats.getFlush().getTotalTimeInMillis();
        if(nodeIndicesStatsBean.flushTotal != 0)
            nodeIndicesStatsBean.flushAvgTimeInMillisPerRequest = nodeIndicesStatsBean.flushTotalTime / nodeIndicesStatsBean.flushTotal;
    }

    private void updateMerge(NodeIndicesStatsBean nodeIndicesStatsBean, NodeIndicesStats nodeIndicesStats)
    {
        nodeIndicesStatsBean.mergesCurrent = nodeIndicesStats.getMerge().getCurrent();
        nodeIndicesStatsBean.mergesCurrentDocs = nodeIndicesStats.getMerge().getCurrentNumDocs();
        nodeIndicesStatsBean.mergesCurrentSize = nodeIndicesStats.getMerge().getCurrentSizeInBytes();
        nodeIndicesStatsBean.mergesTotal = nodeIndicesStats.getMerge().getTotal();
        nodeIndicesStatsBean.mergesTotalTime = nodeIndicesStats.getMerge().getTotalTimeInMillis();
        nodeIndicesStatsBean.mergesTotalSize = nodeIndicesStats.getMerge().getTotalSizeInBytes();
    }

    private void updateCache(NodeIndicesStatsBean nodeIndicesStatsBean, NodeIndicesStats nodeIndicesStats)
    {
        nodeIndicesStatsBean.cacheFieldEvictions = nodeIndicesStats.getFieldData().getEvictions();
        nodeIndicesStatsBean.cacheFieldSize = nodeIndicesStats.getFieldData().getMemorySizeInBytes();
        nodeIndicesStatsBean.cacheFilterEvictions = nodeIndicesStats.getFilterCache().getEvictions();
        nodeIndicesStatsBean.cacheFilterSize = nodeIndicesStats.getFilterCache().getMemorySizeInBytes();
    }

    private void updateSearch(NodeIndicesStatsBean nodeIndicesStatsBean, NodeIndicesStats nodeIndicesStats)
    {
        nodeIndicesStatsBean.searchQueryDelta += (nodeIndicesStats.getSearch().getTotal().getQueryCount() - nodeIndicesStatsBean.searchQueryTotal);
        nodeIndicesStatsBean.searchFetchDelta += (nodeIndicesStats.getSearch().getTotal().getFetchCount() - nodeIndicesStatsBean.searchFetchTotal);

        long searchQueryDeltaTimeInMillies = (nodeIndicesStats.getSearch().getTotal().getQueryTimeInMillis() - nodeIndicesStatsBean.searchQueryTime);
        long queryDelta = nodeIndicesStats.getSearch().getTotal().getQueryCount() - nodeIndicesStatsBean.searchQueryTotal;
        if (queryDelta != 0) {
            recordSearchQueryLatencies(searchQueryDeltaTimeInMillies / queryDelta, TimeUnit.MILLISECONDS);
            nodeIndicesStatsBean.latencySearchQuery95 = latencySearchQuery95Histo.percentile(PERCENTILE_95);
            nodeIndicesStatsBean.latencySearchQuery99 = latencySearchQuery99Histo.percentile(PERCENTILE_99);
        } else {
            nodeIndicesStatsBean.latencySearchQuery95 = 0;
            nodeIndicesStatsBean.latencySearchQuery99 = 0;
        }

        nodeIndicesStatsBean.searchQueryTotal = nodeIndicesStats.getSearch().getTotal().getQueryCount();
        nodeIndicesStatsBean.searchQueryTime = nodeIndicesStats.getSearch().getTotal().getQueryTimeInMillis();
        if(nodeIndicesStatsBean.searchQueryTotal != 0)
            nodeIndicesStatsBean.searchQueryAvgTimeInMillisPerRequest = nodeIndicesStatsBean.searchQueryTime / nodeIndicesStatsBean.searchQueryTotal;
        nodeIndicesStatsBean.searchQueryCurrent = nodeIndicesStats.getSearch().getTotal().getQueryCurrent();
        nodeIndicesStatsBean.searchFetchTotal = nodeIndicesStats.getSearch().getTotal().getFetchCount();

        long searchFetchDeltaTimeInMillies = (nodeIndicesStats.getSearch().getTotal().getFetchTimeInMillis() - nodeIndicesStatsBean.searchFetchTime);
        long fetchDelta =  nodeIndicesStats.getSearch().getTotal().getFetchCount() - nodeIndicesStatsBean.searchFetchTotal;
        if (fetchDelta != 0) {
            recordSearchFetchLatencies(searchFetchDeltaTimeInMillies / fetchDelta, TimeUnit.MILLISECONDS);
            nodeIndicesStatsBean.latencySearchFetch95 = latencySearchFetch95Histo.percentile(PERCENTILE_95);
            nodeIndicesStatsBean.latencySearchFetch99 = latencySearchFetch99Histo.percentile(PERCENTILE_99);
        } else {
            nodeIndicesStatsBean.latencySearchFetch95 = 0;
            nodeIndicesStatsBean.latencySearchFetch99 = 0;
        }

        nodeIndicesStatsBean.searchFetchTime = nodeIndicesStats.getSearch().getTotal().getFetchTimeInMillis();

        if(nodeIndicesStatsBean.searchFetchTotal != 0)
            nodeIndicesStatsBean.searchFetchAvgTimeInMillisPerRequest = nodeIndicesStatsBean.searchFetchTime / nodeIndicesStatsBean.searchFetchTotal;
        nodeIndicesStatsBean.searchFetchCurrent = nodeIndicesStats.getSearch().getTotal().getFetchCurrent();
    }

    private void updateGet(NodeIndicesStatsBean nodeIndicesStatsBean, NodeIndicesStats nodeIndicesStats)
    {
        nodeIndicesStatsBean.getTotalDelta += (nodeIndicesStats.getGet().getCount() - nodeIndicesStatsBean.getTotal);
        nodeIndicesStatsBean.getExistsDelta += (nodeIndicesStats.getGet().getExistsCount() - nodeIndicesStatsBean.getExistsTotal);
        nodeIndicesStatsBean.getMissingDelta += (nodeIndicesStats.getGet().getMissingCount() - nodeIndicesStatsBean.getMissingDelta);

        long getDeltaTimeInMillies = (nodeIndicesStats.getGet().getTimeInMillis() - nodeIndicesStatsBean.getTime);
        long totalDelta = nodeIndicesStats.getGet().getCount() - nodeIndicesStatsBean.getTotal;
        if (totalDelta != 0) {
            recordGetLatencies(getDeltaTimeInMillies / totalDelta, TimeUnit.MILLISECONDS);
            nodeIndicesStatsBean.latencyGet95 = latencyGet95Histo.percentile(PERCENTILE_95);
            nodeIndicesStatsBean.latencyGet99 = latencyGet99Histo.percentile(PERCENTILE_99);
        } else {
            nodeIndicesStatsBean.latencyGet95 = 0;
            nodeIndicesStatsBean.latencyGet99 = 0;
        }

        nodeIndicesStatsBean.getTotal = nodeIndicesStats.getGet().getCount();
        nodeIndicesStatsBean.getTime = nodeIndicesStats.getGet().getTimeInMillis();

        if (nodeIndicesStatsBean.getTotal != 0)
            nodeIndicesStatsBean.getTotalAvgTimeInMillisPerRequest = nodeIndicesStatsBean.getTime / nodeIndicesStatsBean.getTotal;
        nodeIndicesStatsBean.getCurrent = nodeIndicesStats.getGet().current();

        long getExistsDeltaTimeInMillies = (nodeIndicesStats.getGet().getExistsTimeInMillis() - nodeIndicesStatsBean.getExistsTime);
        long existsDelta = nodeIndicesStats.getGet().getExistsCount() - nodeIndicesStatsBean.getExistsTotal;
        if (existsDelta != 0) {
            recordGetExistsLatencies(getExistsDeltaTimeInMillies / existsDelta, TimeUnit.MILLISECONDS);
            nodeIndicesStatsBean.latencyGetExists95 = latencyGetExists95Histo.percentile(PERCENTILE_95);
            nodeIndicesStatsBean.latencyGetExists99 = latencyGetExists99Histo.percentile(PERCENTILE_99);
        } else {
            nodeIndicesStatsBean.latencyGetExists95 = 0;
            nodeIndicesStatsBean.latencyGetExists99 = 0;
        }

        nodeIndicesStatsBean.getExistsTotal = nodeIndicesStats.getGet().getExistsCount();
        nodeIndicesStatsBean.getExistsTime = nodeIndicesStats.getGet().getExistsTimeInMillis();
        if (nodeIndicesStatsBean.getExistsTotal != 0)
            nodeIndicesStatsBean.getExistsAvgTimeInMillisPerRequest = nodeIndicesStatsBean.getExistsTime / nodeIndicesStatsBean.getExistsTotal;

        long getMissingDeltaTimeInMillies = (nodeIndicesStats.getGet().getMissingTimeInMillis() - nodeIndicesStatsBean.getMissingTime);
        long missingDelta = nodeIndicesStats.getGet().getMissingCount() - nodeIndicesStatsBean.getMissingDelta;
        if (missingDelta != 0) {
            recordGetMissingLatencies(getMissingDeltaTimeInMillies / missingDelta, TimeUnit.MILLISECONDS);
            nodeIndicesStatsBean.latencyGetMissing95 = latencyGetMissing95Histo.percentile(PERCENTILE_95);
            nodeIndicesStatsBean.latencyGetMissing99 = latencyGetMissing99Histo.percentile(PERCENTILE_99);
        } else {
            nodeIndicesStatsBean.latencyGetMissing95 = 0;
            nodeIndicesStatsBean.latencyGetMissing99 = 0;
        }

        nodeIndicesStatsBean.getMissingTotal = nodeIndicesStats.getGet().getMissingCount();
        nodeIndicesStatsBean.getMissingTime = nodeIndicesStats.getGet().getMissingTimeInMillis();
        if (nodeIndicesStatsBean.getMissingTotal != 0)
            nodeIndicesStatsBean.getMissingAvgTimeInMillisPerRequest = nodeIndicesStatsBean.getMissingTime / nodeIndicesStatsBean.getMissingTotal;
    }

    private void updateIndexing(NodeIndicesStatsBean nodeIndicesStatsBean, NodeIndicesStats nodeIndicesStats)
    {
        nodeIndicesStatsBean.indexingIndexDelta += (nodeIndicesStats.getIndexing().getTotal().getIndexCount() - nodeIndicesStatsBean.indexingIndexTotal);
        nodeIndicesStatsBean.indexingDeleteDelta += (nodeIndicesStats.getIndexing().getTotal().getDeleteCount() - nodeIndicesStatsBean.indexingDeleteTotal);

        long indexingTimeInMillies = (nodeIndicesStats.getIndexing().getTotal().getIndexTimeInMillis() - nodeIndicesStatsBean.indexingIndexTimeInMillis);
        long indexDelta = (nodeIndicesStats.getIndexing().getTotal().getIndexCount() - nodeIndicesStatsBean.indexingIndexTotal);
        if (indexDelta != 0) {
            recordIndexingLatencies(indexingTimeInMillies / indexDelta, TimeUnit.MILLISECONDS);
            nodeIndicesStatsBean.latencyIndexing95 = latencyIndexing95Histo.percentile(PERCENTILE_95);
            nodeIndicesStatsBean.latencyIndexing99 = latencyIndexing99Histo.percentile(PERCENTILE_99);
        } else {
            nodeIndicesStatsBean.latencyIndexing95 = 0;
            nodeIndicesStatsBean.latencyIndexing99 = 0;
        }

        nodeIndicesStatsBean.indexingIndexTimeInMillis = nodeIndicesStats.getIndexing().getTotal().getIndexTimeInMillis();
        if (nodeIndicesStatsBean.indexingIndexTotal != 0)
            nodeIndicesStatsBean.indexingAvgTimeInMillisPerRequest = nodeIndicesStatsBean.indexingIndexTimeInMillis / nodeIndicesStatsBean.indexingIndexTotal;
        nodeIndicesStatsBean.indexingIndexCurrent = nodeIndicesStats.getIndexing().getTotal().getIndexCurrent();

        long indexDeleteTimeInMillies = (nodeIndicesStats.getIndexing().getTotal().getDeleteTimeInMillis() - nodeIndicesStatsBean.indexingDeleteTime);
        long indexDeleteDelta = nodeIndicesStats.getIndexing().getTotal().getDeleteCount() - nodeIndicesStatsBean.indexingDeleteTotal;
        if (indexDeleteDelta != 0) {
            recordIndexDeleteLatencies(indexDeleteTimeInMillies / indexDeleteDelta, TimeUnit.MILLISECONDS);
            nodeIndicesStatsBean.latencyIndexDelete95 = latencyIndexDelete95Histo.percentile(PERCENTILE_95);
            nodeIndicesStatsBean.latencyIndexDelete99 = latencyIndexDelete99Histo.percentile(PERCENTILE_99);
        } else {
            nodeIndicesStatsBean.latencyIndexDelete95 = 0;
            nodeIndicesStatsBean.latencyIndexDelete99 = 0;
        }

        nodeIndicesStatsBean.indexingDeleteTime = nodeIndicesStats.getIndexing().getTotal().getDeleteTimeInMillis();
        if (nodeIndicesStatsBean.indexingDeleteTotal != 0)
            nodeIndicesStatsBean.indexingDeleteAvgTimeInMillisPerRequest = nodeIndicesStatsBean.indexingDeleteTime / nodeIndicesStatsBean.indexingDeleteTotal;
        nodeIndicesStatsBean.indexingDeleteCurrent = nodeIndicesStats.getIndexing().getTotal().getDeleteCurrent();
        nodeIndicesStatsBean.indexingIndexTotal = nodeIndicesStats.getIndexing().getTotal().getIndexCount();
        nodeIndicesStatsBean.indexingDeleteTotal = nodeIndicesStats.getIndexing().getTotal().getDeleteCount();
    }

    private void recordSearchQueryLatencies(long duration, TimeUnit unit) {
        long searchQueryLatency = TimeUnit.MICROSECONDS.convert(duration, unit);
        latencySearchQuery95Histo.add(searchQueryLatency);
        latencySearchQuery99Histo.add(searchQueryLatency);
    }

    private void recordSearchFetchLatencies(long duration, TimeUnit unit) {
        long fetchQueryLatency = TimeUnit.MICROSECONDS.convert(duration, unit);
        latencySearchFetch95Histo.add(fetchQueryLatency);
        latencySearchFetch99Histo.add(fetchQueryLatency);
    }

    private void recordGetLatencies(long duration, TimeUnit unit) {
        long getLatency = TimeUnit.MICROSECONDS.convert(duration, unit);
        latencyGet95Histo.add(getLatency);
        latencyGet99Histo.add(getLatency);
    }

    private void recordGetExistsLatencies(long duration, TimeUnit unit) {
        long getExistsLatency = TimeUnit.MICROSECONDS.convert(duration, unit);
        latencyGetExists95Histo.add(getExistsLatency);
        latencyGetExists99Histo.add(getExistsLatency);
    }

    private void recordGetMissingLatencies(long duration, TimeUnit unit) {
        long getMissingLatency = TimeUnit.MICROSECONDS.convert(duration, unit);
        latencyGetMissing95Histo.add(getMissingLatency);
        latencyGetMissing99Histo.add(getMissingLatency);
    }

    private void recordIndexingLatencies(long duration, TimeUnit unit) {
        long indexingLatency = TimeUnit.MICROSECONDS.convert(duration, unit);
        latencyIndexing95Histo.add(indexingLatency);
        latencyIndexing99Histo.add(indexingLatency);
    }

    private void recordIndexDeleteLatencies(long duration, TimeUnit unit) {
        long indexDeleteLatency = TimeUnit.MICROSECONDS.convert(duration, unit);
        latencyIndexDelete95Histo.add(indexDeleteLatency);
        latencyIndexDelete99Histo.add(indexDeleteLatency);
    }

    public class Elasticsearch_NodeIndicesStatsReporter
    {
        private final AtomicReference<NodeIndicesStatsBean> nodeIndicesStatsBean;

        public Elasticsearch_NodeIndicesStatsReporter()
        {
            nodeIndicesStatsBean = new AtomicReference<NodeIndicesStatsBean>(new NodeIndicesStatsBean());
        }

        @Monitor(name ="store_size", type=DataSourceType.GAUGE)
        public long getStoreSize()
        {
            return nodeIndicesStatsBean.get().storeSize;
        }
        @Monitor(name="store_throttle_time", type=DataSourceType.GAUGE)
        public long getStoreThrottleTime()
        {
            return nodeIndicesStatsBean.get().storeThrottleTime;
        }
        @Monitor(name="docs_count", type=DataSourceType.GAUGE)
        public long getDocsCount()
        {
            return nodeIndicesStatsBean.get().docsCount;
        }
        @Monitor(name="docs_deleted", type=DataSourceType.GAUGE)
        public long getDocsDeleted()
        {
            return nodeIndicesStatsBean.get().docsDeleted;
        }


        //Indexing
        @Monitor(name="indexing_index_total", type=DataSourceType.GAUGE)
        public long getIndexingIndexTotal() { return nodeIndicesStatsBean.get().indexingIndexTotal; }
        @Monitor(name="indexing_index_time_in_millis", type=DataSourceType.GAUGE)
        public long getIndexingIndexTimeInMillis()
        {
            return nodeIndicesStatsBean.get().indexingIndexTimeInMillis;
        }
        @Monitor(name="indexing_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getIndexingAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().indexingAvgTimeInMillisPerRequest;
        }

        @Monitor(name="indexing_index_current", type=DataSourceType.GAUGE)
        public long getIndexingIndexCurrent()
        {
            return nodeIndicesStatsBean.get().indexingIndexCurrent;
        }

        @Monitor(name="indexing_delete_total", type=DataSourceType.GAUGE)
        public long getIndexingDeleteTotal()
        {
            return nodeIndicesStatsBean.get().indexingDeleteTotal;
        }
        @Monitor(name="indexing_delete_time", type=DataSourceType.GAUGE)
        public long getIndexingDeleteTime()
        {
            return nodeIndicesStatsBean.get().indexingDeleteTime;
        }
        @Monitor(name="indexing_delete_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getIndexingDeleteAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().indexingDeleteAvgTimeInMillisPerRequest;
        }

        @Monitor(name="indexing_delete_current", type=DataSourceType.GAUGE)
        public long getIndexingDeleteCurrent()
        {
            return nodeIndicesStatsBean.get().indexingDeleteCurrent;
        }
        @Monitor(name="indexing_index_delta", type=DataSourceType.COUNTER)
        public long getIndexingIndexDelta()
        {
            return nodeIndicesStatsBean.get().indexingIndexDelta;
        }
        @Monitor(name="indexing_delete_delta", type=DataSourceType.COUNTER)
        public long getIndexingDeleteDelta()
        {
            return nodeIndicesStatsBean.get().indexingDeleteDelta;
        }

        //Get
        @Monitor(name="get_total", type=DataSourceType.GAUGE)
        public long getGetTotal()
        {
            return nodeIndicesStatsBean.get().getTotal;
        }
        @Monitor(name="get_time", type=DataSourceType.GAUGE)
        public long getGetTime()
        {
            return nodeIndicesStatsBean.get().getTime;
        }
        @Monitor(name="total_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getTotalAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().getTotalAvgTimeInMillisPerRequest;
        }


        @Monitor(name="get_current", type=DataSourceType.GAUGE)
        public long getGetCurrent()
        {
            return nodeIndicesStatsBean.get().getCurrent;
        }
        @Monitor(name="get_exists_total", type=DataSourceType.GAUGE)
        public long getGetExistsTotal()
        {
            return nodeIndicesStatsBean.get().getExistsTotal;
        }
        @Monitor(name="get_exists_time", type=DataSourceType.GAUGE)
        public long getGetExistsTime()
        {
            return nodeIndicesStatsBean.get().getExistsTime;
        }
        @Monitor(name="exists_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getExistsAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().getExistsAvgTimeInMillisPerRequest;
        }


        @Monitor(name="get_missing_total", type=DataSourceType.GAUGE)
        public long getGetMissingTotal()
        {
            return nodeIndicesStatsBean.get().getMissingTotal;
        }
        @Monitor(name="get_missing_time", type=DataSourceType.GAUGE)
        public long getGetMissingTime()
        {
            return nodeIndicesStatsBean.get().getMissingTime;
        }
        @Monitor(name="missing_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getMissingAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().getMissingAvgTimeInMillisPerRequest;
        }

        //Search
        @Monitor(name="get_total_delta", type=DataSourceType.COUNTER)
        public long getGetTotalDelta()
        {
            return nodeIndicesStatsBean.get().getTotalDelta;
        }
        @Monitor(name="get_exists_delta", type=DataSourceType.COUNTER)
        public long getGetExistsDelta()
        {
            return nodeIndicesStatsBean.get().getExistsDelta;
        }
        @Monitor(name="get_missing_delta", type=DataSourceType.COUNTER)
        public long getGetMissingDelta()
        {
            return nodeIndicesStatsBean.get().getMissingDelta;
        }

        @Monitor(name="search_query_total", type=DataSourceType.GAUGE)
        public long getSearchQueryTotal()
        {
            return nodeIndicesStatsBean.get().searchQueryTotal;
        }
        @Monitor(name="search_query_time", type=DataSourceType.GAUGE)
        public long getSearchQueryTime()
        {
            return nodeIndicesStatsBean.get().searchQueryTime;
        }
        @Monitor(name="search_query_current", type=DataSourceType.GAUGE)
        public long getSearchQueryCurrent()
        {
            return nodeIndicesStatsBean.get().searchQueryCurrent;
        }
        @Monitor(name="search_query_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getSearchQueryAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().searchQueryAvgTimeInMillisPerRequest;
        }

        @Monitor(name="search_query_delta", type=DataSourceType.COUNTER)
        public long getSearchQueryDelta()
        {
            return nodeIndicesStatsBean.get().searchQueryDelta;
        }
        @Monitor(name="search_fetch_total", type=DataSourceType.GAUGE)
        public long getSearchFetchTotal()
        {
            return nodeIndicesStatsBean.get().searchFetchTotal;
        }
        @Monitor(name="search_fetch_time", type=DataSourceType.GAUGE)
        public long getSearchFetchTime()
        {
            return nodeIndicesStatsBean.get().searchFetchTime;
        }
        @Monitor(name="search_fetch_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getSearchFetchAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().searchFetchAvgTimeInMillisPerRequest;
        }

        @Monitor(name="search_fetch_current", type=DataSourceType.GAUGE)
        public long getSearchFetchCurrent()
        {
            return nodeIndicesStatsBean.get().searchFetchCurrent;
        }
        @Monitor(name="search_fetch_delta", type=DataSourceType.COUNTER)
        public long getSearchFetchDelta()
        {
            return nodeIndicesStatsBean.get().searchFetchDelta;
        }

        //Cache
        @Monitor(name="cache_field_evictions", type=DataSourceType.GAUGE)
        public long getCacheFieldEvictions()
        {
            return nodeIndicesStatsBean.get().cacheFieldEvictions;
        }
        @Monitor(name="cache_field_size", type=DataSourceType.GAUGE)
        public long getCacheFieldSize()
        {
            return nodeIndicesStatsBean.get().cacheFieldSize;
        }
        @Monitor(name="cache_filter_evictions", type=DataSourceType.GAUGE)
        public long getCacheFilterEvictions()
        {
            return nodeIndicesStatsBean.get().cacheFilterEvictions;
        }
        @Monitor(name="cache_filter_size", type=DataSourceType.GAUGE)
        public long getCacheFilterSize()
        {
            return nodeIndicesStatsBean.get().cacheFilterSize;
        }

        //Merge
        @Monitor(name="merges_current", type=DataSourceType.GAUGE)
        public long getMergesCurrent()
        {
            return nodeIndicesStatsBean.get().mergesCurrent;
        }
        @Monitor(name="merges_current_docs", type=DataSourceType.GAUGE)
        public long getMergesCurrentDocs()
        {
            return nodeIndicesStatsBean.get().mergesCurrentDocs;
        }
        @Monitor(name="merges_current_size", type=DataSourceType.GAUGE)
        public long getMergesCurrentSize()
        {
            return nodeIndicesStatsBean.get().mergesCurrentSize;
        }
        @Monitor(name="merges_total", type=DataSourceType.GAUGE)
        public long getMergesTotal()
        {
            return nodeIndicesStatsBean.get().mergesTotal;
        }
        @Monitor(name="merges_total_time", type=DataSourceType.GAUGE)
        public long getMergesTotalTime()
        {
            return nodeIndicesStatsBean.get().mergesTotalTime;
        }
        @Monitor(name="merges_total_size", type=DataSourceType.GAUGE)
        public long getMergesTotalSize()
        {
            return nodeIndicesStatsBean.get().mergesTotalSize;
        }

        //Refresh
        @Monitor(name="refresh_total", type=DataSourceType.GAUGE)
        public long getRefreshTotal()
        {
            return nodeIndicesStatsBean.get().refreshTotal;
        }
        @Monitor(name="refresh_total_time", type=DataSourceType.GAUGE)
        public long getRefreshTotalTime()
        {
            return nodeIndicesStatsBean.get().refreshTotalTime;
        }
        @Monitor(name="refresh_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getRefreshAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().refreshAvgTimeInMillisPerRequest;
        }

        //Flush
        @Monitor(name="flush_total", type=DataSourceType.GAUGE)
        public long getFlushTotal()
        {
            return nodeIndicesStatsBean.get().flushTotal;
        }
        @Monitor(name="flush_total_time", type=DataSourceType.GAUGE)
        public long getFlushTotalTime()
        {
            return nodeIndicesStatsBean.get().flushTotalTime;
        }
        @Monitor(name="flush_avg_time_in_millis_per_request", type=DataSourceType.GAUGE)
        public double getFlushAvgTimeInMillisPerRequest()
        {
            return nodeIndicesStatsBean.get().flushAvgTimeInMillisPerRequest;
        }

        //Percentile Latencies
        @Monitor(name="latencySearchQuery95", type=DataSourceType.GAUGE)
        public double getLatencySearchQuery95() { return nodeIndicesStatsBean.get().latencySearchQuery95; }
        @Monitor(name="latencySearchQuery99", type=DataSourceType.GAUGE)
        public double getLatencySearchQuery99() { return nodeIndicesStatsBean.get().latencySearchQuery99; }
        @Monitor(name="latencySearchFetch95", type=DataSourceType.GAUGE)
        public double getLatencySearchFetch95() { return nodeIndicesStatsBean.get().latencySearchFetch95; }
        @Monitor(name="latencySearchFetch99", type=DataSourceType.GAUGE)
        public double getLatencySearchFetch99() { return nodeIndicesStatsBean.get().latencySearchFetch99; }
        @Monitor(name="latencyGet95", type=DataSourceType.GAUGE)
        public double getLatencyGet95() { return nodeIndicesStatsBean.get().latencyGet95; }
        @Monitor(name="latencyGet99", type=DataSourceType.GAUGE)
        public double getLatencyGet99() { return nodeIndicesStatsBean.get().latencyGet99; }
        @Monitor(name="latencyGetExists95", type=DataSourceType.GAUGE)
        public double getLatencyGetExists95() { return nodeIndicesStatsBean.get().latencyGetExists95; }
        @Monitor(name="latencyGetExists99", type=DataSourceType.GAUGE)
        public double getLatencyGetExists99() { return nodeIndicesStatsBean.get().latencyGetExists99; }
        @Monitor(name="latencyGetMissing95", type=DataSourceType.GAUGE)
        public double getLatencyGetMissing95() { return nodeIndicesStatsBean.get().latencyGetMissing95; }
        @Monitor(name="latencyGetMissing99", type=DataSourceType.GAUGE)
        public double getLatencyGetMissing99() { return nodeIndicesStatsBean.get().latencyGetMissing99; }
        @Monitor(name="latencyIndexing95", type=DataSourceType.GAUGE)
        public double getLatencyIndexing95() { return nodeIndicesStatsBean.get().latencyIndexing95; }
        @Monitor(name="latencyIndexing99", type=DataSourceType.GAUGE)
        public double getLatencyIndexing99() { return nodeIndicesStatsBean.get().latencyIndexing99; }
        @Monitor(name="latencyIndexDelete95", type=DataSourceType.GAUGE)
        public double getLatencyIndexDelete95() { return nodeIndicesStatsBean.get().latencyIndexDelete95; }
        @Monitor(name="latencyIndexDelete99", type=DataSourceType.GAUGE)
        public double getLatencyIndexDelete99() { return nodeIndicesStatsBean.get().latencyIndexDelete99; }
    }


    private static class NodeIndicesStatsBean
    {
        private long storeSize = -1;
        private long storeThrottleTime = -1;
        private long docsCount = -1;
        private long docsDeleted = -1;
        private long indexingIndexTotal = -1;
        private long indexingIndexTimeInMillis = -1;
        private double indexingAvgTimeInMillisPerRequest = -1;
        private long indexingIndexCurrent = -1;
        private long indexingDeleteTotal = -1;
        private long indexingDeleteTime = -1;
        private double indexingDeleteAvgTimeInMillisPerRequest = -1;
        private long indexingDeleteCurrent = -1;
        private long indexingIndexDelta = -1;
        private long indexingDeleteDelta = -1;
        private long getTotal = -1;
        private long getTime = -1;
        private double getTotalAvgTimeInMillisPerRequest = -1;
        private long getCurrent = -1;
        private long getExistsTotal = -1;
        private long getExistsTime = -1;
        private double getExistsAvgTimeInMillisPerRequest = -1;
        private long getMissingTotal = -1;
        private long getMissingTime = -1;
        private double getMissingAvgTimeInMillisPerRequest = -1;
        private long getTotalDelta = -1;
        private long getExistsDelta = -1;
        private long getMissingDelta = -1;
        private long searchQueryTotal = -1;
        private long searchQueryTime = -1;
        private double searchQueryAvgTimeInMillisPerRequest = -1;
        private long searchQueryCurrent = -1;
        private long searchQueryDelta = -1;
        private long searchFetchTotal = -1;
        private long searchFetchTime = -1;
        private double searchFetchAvgTimeInMillisPerRequest = -1;
        private long searchFetchCurrent = -1;
        private long searchFetchDelta = -1;
        private long cacheFieldEvictions = -1;
        private long cacheFieldSize = -1;
        private long cacheFilterEvictions = -1;
        private long cacheFilterSize = -1;
        private long mergesCurrent = -1;
        private long mergesCurrentDocs = -1;
        private long mergesCurrentSize = -1;
        private long mergesTotal = -1;
        private long mergesTotalTime = -1;
        private long mergesTotalSize = -1;
        private long refreshTotal = -1;
        private long refreshTotalTime = -1;
        private double refreshAvgTimeInMillisPerRequest = -1;
        private long flushTotal = -1;
        private long flushTotalTime = -1;
        private double flushAvgTimeInMillisPerRequest = -1;
        private double latencySearchQuery95 = -1;
        private double latencySearchQuery99 = -1;
        private double latencySearchFetch95 = -1;
        private double latencySearchFetch99 = -1;
        private double latencyGet95 = -1;
        private double latencyGet99 = -1;
        private double latencyGetExists95 = -1;
        private double latencyGetExists99 = -1;
        private double latencyGetMissing95 = -1;
        private double latencyGetMissing99 = -1;
        private double latencyIndexing95 = -1;
        private double latencyIndexing99 = -1;
        private double latencyIndexDelete95 = -1;
        private double latencyIndexDelete99 = -1;
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

    private void resetNodeIndicesStats(NodeIndicesStatsBean nodeIndicesStatsBean){
        nodeIndicesStatsBean.storeSize = -1;
        nodeIndicesStatsBean.storeThrottleTime = -1;
        nodeIndicesStatsBean.docsCount = -1;
        nodeIndicesStatsBean.docsDeleted = -1;
        nodeIndicesStatsBean.indexingIndexTotal = -1;
        nodeIndicesStatsBean.indexingIndexTimeInMillis = -1;
        nodeIndicesStatsBean.indexingAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.indexingIndexCurrent = -1;
        nodeIndicesStatsBean.indexingDeleteTotal = -1;
        nodeIndicesStatsBean.indexingDeleteTime = -1;
        nodeIndicesStatsBean.indexingDeleteAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.indexingDeleteCurrent = -1;
        nodeIndicesStatsBean.indexingIndexDelta = -1;
        nodeIndicesStatsBean.indexingDeleteDelta = -1;
        nodeIndicesStatsBean.getTotal = -1;
        nodeIndicesStatsBean.getTime = -1;
        nodeIndicesStatsBean.getTotalAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.getCurrent = -1;
        nodeIndicesStatsBean.getExistsTotal = -1;
        nodeIndicesStatsBean.getExistsTime = -1;
        nodeIndicesStatsBean.getExistsAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.getMissingTotal = -1;
        nodeIndicesStatsBean.getMissingTime = -1;
        nodeIndicesStatsBean.getMissingAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.getTotalDelta = -1;
        nodeIndicesStatsBean.getExistsDelta = -1;
        nodeIndicesStatsBean.getMissingDelta = -1;
        nodeIndicesStatsBean.searchQueryTotal = -1;
        nodeIndicesStatsBean.searchQueryTime = -1;
        nodeIndicesStatsBean.searchQueryAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.searchQueryCurrent = -1;
        nodeIndicesStatsBean.searchQueryDelta = -1;
        nodeIndicesStatsBean.searchFetchTotal = -1;
        nodeIndicesStatsBean.searchFetchTime = -1;
        nodeIndicesStatsBean.searchFetchAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.searchFetchCurrent = -1;
        nodeIndicesStatsBean.searchFetchDelta = -1;
        nodeIndicesStatsBean.cacheFieldEvictions = -1;
        nodeIndicesStatsBean.cacheFieldSize = -1;
        nodeIndicesStatsBean.cacheFilterEvictions = -1;
        nodeIndicesStatsBean.cacheFilterSize = -1;
        nodeIndicesStatsBean.mergesCurrent = -1;
        nodeIndicesStatsBean.mergesCurrentDocs = -1;
        nodeIndicesStatsBean.mergesCurrentSize = -1;
        nodeIndicesStatsBean.mergesTotal = -1;
        nodeIndicesStatsBean.mergesTotalTime = -1;
        nodeIndicesStatsBean.mergesTotalSize = -1;
        nodeIndicesStatsBean.refreshTotal = -1;
        nodeIndicesStatsBean.refreshTotalTime = -1;
        nodeIndicesStatsBean.refreshAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.flushTotal = -1;
        nodeIndicesStatsBean.flushTotalTime = -1;
        nodeIndicesStatsBean.flushAvgTimeInMillisPerRequest = -1;
        nodeIndicesStatsBean.latencySearchQuery95 = -1;
        nodeIndicesStatsBean.latencySearchQuery99 = -1;
        nodeIndicesStatsBean.latencySearchFetch95 = -1;
        nodeIndicesStatsBean.latencySearchFetch99 = -1;
        nodeIndicesStatsBean.latencyGet95 = -1;
        nodeIndicesStatsBean.latencyGet99 = -1;
        nodeIndicesStatsBean.latencyGetExists95 = -1;
        nodeIndicesStatsBean.latencyGetExists99 = -1;
        nodeIndicesStatsBean.latencyGetMissing95 = -1;
        nodeIndicesStatsBean.latencyGetMissing99 = -1;
        nodeIndicesStatsBean.latencyIndexing95 = -1;
        nodeIndicesStatsBean.latencyIndexing99 = -1;
        nodeIndicesStatsBean.latencyIndexDelete95 = -1;
        nodeIndicesStatsBean.latencyIndexDelete99 = -1;
    }
}
