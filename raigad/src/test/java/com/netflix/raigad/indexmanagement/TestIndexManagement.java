package com.netflix.raigad.indexmanagement;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.configuration.UnitTestModule;
import com.netflix.raigad.utils.ElasticsearchTransportClient;
import mockit.Mock;
import mockit.Mocked;
import mockit.Mockit;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Ignore
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class TestIndexManagement extends ESIntegTestCase {
    private static final DateTimeZone currentZone = DateTimeZone.UTC;
    private static final String S3_REPO_DATE_FORMAT = "yyyyMMdd";
    private static final String indexPrefix = "test_index";
    private static final int numDays = 5;

    private static Injector injector;
    public static Client client0;

    @Mocked
    private static ElasticsearchTransportClient esTransportClient;

    private static IConfiguration conf;
    @Mocked
    private static ESIndexManager esIndexManager;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        injector = Guice.createInjector(new UnitTestModule());
        conf = injector.getInstance(IConfiguration.class);

        Mockit.setUpMock(ElasticsearchTransportClient.class, MockElasticsearchTransportClient.class);
        esTransportClient = injector.getInstance(ElasticsearchTransportClient.class);

        Mockit.setUpMock(ESIndexManager.class, MockESIndexManager.class);
        if (esIndexManager == null) {
            esIndexManager = injector.getInstance(ESIndexManager.class);
        }
    }

    @AfterClass
    public static void cleanup() throws IOException {
        injector = null;
        client0 = null;
        esTransportClient = null;
        conf = null;
        esIndexManager = null;
    }

    @Ignore
    public static class MockElasticsearchTransportClient {
        @Mock
        public static ElasticsearchTransportClient instance(IConfiguration config) {
            return esTransportClient;
        }

        @Mock
        public Client getTransportClient() {
            return client0;
        }
    }

    @Ignore
    public static class MockESIndexManager {
        @Mock
        public IndicesStatsResponse getIndicesStatusResponse(Client esTransportClient) {
            return getLocalIndicesStatusResponse();
        }

        @Mock
        public void deleteIndices(Client client, String indexName, int timeout) {
            client0.admin().indices().prepareDelete(indexName).execute().actionGet(timeout);
        }
    }

    @Test
    public void testIndexRetentionWithPreCreate() throws Exception {
        client0 = client();

        Map<String, IndexStats> beforeIndexStatusMap = getLocalIndicesStatusResponse().getIndices();
        assertEquals(0, beforeIndexStatusMap.size());

        //Create Old indices for {numDays}
        createOldIndices(indexPrefix, numDays);

        Map<String, IndexStats> afterIndexStatusMap = getLocalIndicesStatusResponse().getIndices();
        assertEquals(numDays, afterIndexStatusMap.size());

        esIndexManager.runIndexManagement();

        Map<String, IndexStats> finalIndexStatusMap = getLocalIndicesStatusResponse().getIndices();

        List<IndexMetadata> indexMetadataList = ESIndexManager.buildInfo(conf.getIndexMetadata());

        /**
         * If pre-create is enabled, it will create today's index + (retention period in days - 1) day indices for future days
         */
        if (indexMetadataList.get(0).isPreCreate()) {
            assertEquals((indexMetadataList.get(0).getRetentionPeriod() - 1) * 2 + 1, finalIndexStatusMap.size());
        } else {
            assertEquals(indexMetadataList.get(0).getRetentionPeriod() - 1, finalIndexStatusMap.size());
        }
    }

    public static IndicesStatsResponse getLocalIndicesStatusResponse() {
        return client0.admin().indices().prepareStats().execute().actionGet(conf.getAutoCreateIndexTimeout());
    }

    public static void createOldIndices(String indexPrefix, int numDays) {
        for (int i = numDays; i > 0; i--) {
            String indexName = indexPrefix + getFormattedDate(i);
            client0.admin().indices().prepareCreate(indexName).execute().actionGet();
        }
    }

    public static String getFormattedDate(int priorDay) {
        DateTime dt = new DateTime().minusDays(priorDay).withZone(currentZone);
        DateTimeFormatter fmt = DateTimeFormat.forPattern(S3_REPO_DATE_FORMAT);
        return dt.toString(fmt);
    }
}
