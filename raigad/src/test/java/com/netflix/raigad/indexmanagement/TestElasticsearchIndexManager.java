package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.indexmanagement.exception.UnsupportedAutoIndexException;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;

import static org.mockito.Mockito.*;

public class TestElasticsearchIndexManager {
    private static final int AUTO_CREATE_INDEX_TIMEOUT = 300000;

    private Client elasticsearchClient;
    private IConfiguration config;

    private ElasticsearchIndexManager elasticsearchIndexManager;

    @Before
    public void setUp() throws Exception {
        config = mock(IConfiguration.class);
        when(config.getAutoCreateIndexTimeout()).thenReturn(AUTO_CREATE_INDEX_TIMEOUT);

        elasticsearchClient = mock(Client.class);

        elasticsearchIndexManager = spy(new ElasticsearchIndexManager(config, null));
        doReturn(elasticsearchClient).when(elasticsearchIndexManager).getTransportClient();

        doNothing().when(elasticsearchIndexManager).deleteIndices(eq(elasticsearchClient), anyString(), anyInt());
    }

    @Test
    public void testRunIndexManagement_NotActionable_NoIndex() throws Exception {
        String serializedIndexMetadata = "[{\"retentionType\": \"yearly\", \"retentionPeriod\": 20}]";
        when(config.getIndexMetadata()).thenReturn(serializedIndexMetadata);

        Map<String, IndexStats> indexStats = new HashMap<>();
        indexStats.put("nf_errors_log2018", new IndexStats("nf_errors_log2018", new ShardStats[]{}));

        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        when(indicesStatsResponse.getIndices()).thenReturn(indexStats);

        doReturn(indicesStatsResponse).when(elasticsearchIndexManager).getIndicesStatsResponse(elasticsearchClient);

        elasticsearchIndexManager.runIndexManagement();

        verify(elasticsearchIndexManager, times(0)).checkIndexRetention(any(Client.class), anySet(), any(IndexMetadata.class), any(DateTime.class));
        verify(elasticsearchIndexManager, times(0)).preCreateIndex(any(Client.class), any(IndexMetadata.class), any(DateTime.class));
    }

    @Test
    public void testRunIndexManagement_NotActionable_NoRetentionPeriod() throws Exception {
        String serializedIndexMetadata = "[{\"retentionType\": \"yearly\", \"indexName\": \"nf_errors_log\"}]";
        when(config.getIndexMetadata()).thenReturn(serializedIndexMetadata);

        Map<String, IndexStats> indexStats = new HashMap<>();
        indexStats.put("nf_errors_log2018", new IndexStats("nf_errors_log2018", new ShardStats[]{}));

        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        when(indicesStatsResponse.getIndices()).thenReturn(indexStats);

        doReturn(indicesStatsResponse).when(elasticsearchIndexManager).getIndicesStatsResponse(elasticsearchClient);

        elasticsearchIndexManager.runIndexManagement();

        verify(elasticsearchIndexManager, times(0)).checkIndexRetention(any(Client.class), anySet(), any(IndexMetadata.class), any(DateTime.class));
        verify(elasticsearchIndexManager, times(0)).preCreateIndex(any(Client.class), any(IndexMetadata.class), any(DateTime.class));
    }

    @Test
    public void testRunIndexManagement() throws Exception {
        String serializedIndexMetadata = "[{\"retentionType\": \"yearly\", \"retentionPeriod\": 3, \"indexName\": \"nf_errors_log\"}]";
        when(config.getIndexMetadata()).thenReturn(serializedIndexMetadata);

        Map<String, IndexStats> indexStats = new HashMap<>();
        indexStats.put("nf_errors_log2018", new IndexStats("nf_errors_log2018", new ShardStats[]{}));
        indexStats.put("nf_errors_log2017", new IndexStats("nf_errors_log2017", new ShardStats[]{}));
        indexStats.put("nf_errors_log2016", new IndexStats("nf_errors_log2016", new ShardStats[]{}));
        indexStats.put("nf_errors_log2015", new IndexStats("nf_errors_log2015", new ShardStats[]{}));
        indexStats.put("nf_errors_log2014", new IndexStats("nf_errors_log2014", new ShardStats[]{}));
        indexStats.put("nf_errors_log2013", new IndexStats("nf_errors_log2013", new ShardStats[]{}));
        indexStats.put("nf_errors_log2012", new IndexStats("nf_errors_log2012", new ShardStats[]{}));

        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        when(indicesStatsResponse.getIndices()).thenReturn(indexStats);

        doReturn(indicesStatsResponse).when(elasticsearchIndexManager).getIndicesStatsResponse(elasticsearchClient);

        elasticsearchIndexManager.runIndexManagement();

        verify(elasticsearchIndexManager, times(1)).checkIndexRetention(any(Client.class), anySet(), any(IndexMetadata.class), any(DateTime.class));

        verify(elasticsearchIndexManager, times(1)).deleteIndices(any(Client.class), eq("nf_errors_log2012"), eq(AUTO_CREATE_INDEX_TIMEOUT));
        verify(elasticsearchIndexManager, times(1)).deleteIndices(any(Client.class), eq("nf_errors_log2013"), eq(AUTO_CREATE_INDEX_TIMEOUT));

        verify(elasticsearchIndexManager, times(0)).preCreateIndex(any(Client.class), any(IndexMetadata.class), any(DateTime.class));
    }

    @Test
    public void testCheckIndexRetention_Hourly() throws IOException, UnsupportedAutoIndexException {
        String serializedIndexMetadata = "[{\"preCreate\": false, \"retentionType\": \"hourly\", \"retentionPeriod\": 2, \"indexName\": \"nf_errors_log\"}]";
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(serializedIndexMetadata);
        IndexMetadata indexMetadata = indexMetadataList.get(0);

        Set<String> indices = new HashSet<>(
                Arrays.asList("nf_errors_log2017062210", "nf_errors_log2017062211", "nf_errors_log2017062212", "nf_errors_log2017062213", "nf_errors_log2017062214"));

        elasticsearchIndexManager.checkIndexRetention(elasticsearchClient, indices, indexMetadata, new DateTime("2017-06-22T13:30Z"));

        verify(elasticsearchIndexManager, times(1)).deleteIndices(any(Client.class), eq("nf_errors_log2017062210"), eq(AUTO_CREATE_INDEX_TIMEOUT));
    }

    @Test
    public void testCheckIndexRetention_Overlapping() throws Exception {
        String serializedIndexMetadata = "[{\"preCreate\": false, \"retentionType\": \"hourly\", \"retentionPeriod\": 2, \"indexName\": \"nf_errors_log\"}," +
                "{\"preCreate\": false, \"retentionType\": \"yearly\", \"retentionPeriod\": 3, \"indexName\": \"nf_errors_log201712\"}]";
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(serializedIndexMetadata);

        Map<String, IndexStats> indexStats = new HashMap<>();
        indexStats.put("nf_errors_log2017121110", new IndexStats("nf_errors_log2017121110", new ShardStats[]{}));
        indexStats.put("nf_errors_log2017121111", new IndexStats("nf_errors_log2017121111", new ShardStats[]{}));
        indexStats.put("nf_errors_log2017121112", new IndexStats("nf_errors_log2017121112", new ShardStats[]{}));
        indexStats.put("nf_errors_log2017121113", new IndexStats("nf_errors_log2017121113", new ShardStats[]{}));
        indexStats.put("nf_errors_log2017121114", new IndexStats("nf_errors_log2017121114", new ShardStats[]{}));

        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        when(indicesStatsResponse.getIndices()).thenReturn(indexStats);

        doReturn(indicesStatsResponse).when(elasticsearchIndexManager).getIndicesStatsResponse(elasticsearchClient);

        elasticsearchIndexManager.runIndexManagement(elasticsearchClient, indexMetadataList, new DateTime("2017-12-11T13:30Z"));

        verify(elasticsearchIndexManager, times(2)).checkIndexRetention(any(Client.class), anySet(), any(IndexMetadata.class), any(DateTime.class));
    }

    @After
    public void cleanUp() throws Exception {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(
                new ObjectName("com.netflix.raigad.scheduler:type=" + ElasticsearchIndexManager.class.getName()));
    }
}