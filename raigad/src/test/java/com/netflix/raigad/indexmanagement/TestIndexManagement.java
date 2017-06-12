package com.netflix.raigad.indexmanagement;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.configuration.UnitTestModule;
import com.netflix.raigad.utils.ESTransportClient;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.elasticsearch.action.admin.indices.status.IndexStatus;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Ignore
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 2)
public class TestIndexManagement extends ElasticsearchIntegrationTest {

    private static final DateTimeZone currentZone = DateTimeZone.UTC;
    private static final String S3_REPO_DATE_FORMAT = "yyyyMMdd";
    private static final String indexPrefix = "test_index";
    private static final int numDays = 5;

    private static Injector injector;
    private static Client client0;
    private static IConfiguration configuration;

    @Mocked
    private static ESTransportClient esTransportClient;

    @Mocked
    private static ElasticSearchIndexManager esIndexManager;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        injector = Guice.createInjector(new UnitTestModule());
        configuration = injector.getInstance(IConfiguration.class);

        esTransportClient = injector.getInstance(ESTransportClient.class);

        if (esIndexManager == null) {
            esIndexManager = injector.getInstance(ElasticSearchIndexManager.class);
        }
    }

    @AfterClass
    public static void cleanup() throws IOException {
        injector = null;
        client0 = null;
        esTransportClient = null;
        configuration = null;
        esIndexManager = null;
    }

    @Ignore
    public static class MockESTransportClient extends MockUp<ESTransportClient> {
        @Mock
        public static ESTransportClient instance(IConfiguration config) {
            return esTransportClient;
        }

        @Mock
        public Client getTransportClient() {
            return client0;
        }
    }

    @Ignore
    public static class MockElasticsearchIndexManager extends MockUp<ElasticSearchIndexManager> {
        @Mock
        public IndicesStatusResponse getIndicesStatusResponse(Client esTransportClient) {
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

        Map<String, IndexStatus> beforeIndexStatusMap = getLocalIndicesStatusResponse().getIndices();
        Assert.assertEquals(0, beforeIndexStatusMap.size());

        //Create Old indices for {numDays}
        createOldIndices(indexPrefix, numDays);

        Map<String, IndexStatus> afterIndexStatusMap = getLocalIndicesStatusResponse().getIndices();
        Assert.assertEquals(numDays, afterIndexStatusMap.size());

        esIndexManager.runIndexManagement();

        Map<String, IndexStatus> finalIndexStatusMap = getLocalIndicesStatusResponse().getIndices();

        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(configuration.getIndexMetadata());

        /**
         * If Pre-Create is Enabled, it will create Today's Index + (Retention Period in Days - 1)day indices for future days
         */
        if (indexMetadataList.get(0).isPreCreate()) {
            Assert.assertEquals((indexMetadataList.get(0).getRetentionPeriod() - 1) * 2 + 1, finalIndexStatusMap.size());
        } else {
            Assert.assertEquals(indexMetadataList.get(0).getRetentionPeriod() - 1, finalIndexStatusMap.size());
        }
    }

    public static IndicesStatusResponse getLocalIndicesStatusResponse() {
        return client0.admin().indices().prepareStatus().execute().actionGet(configuration.getAutoCreateIndexTimeout());
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
