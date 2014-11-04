package com.netflix.elasticcar.indexmanagement;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.elasticcar.configuration.UnitTestModule;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.ESTransportClient;
import mockit.Mock;
import mockit.Mocked;
import mockit.Mockit;
import org.elasticsearch.action.admin.indices.status.IndexStatus;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
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

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes=2)
public class TestIndexManagement extends ElasticsearchIntegrationTest {

    private static final DateTimeZone currentZone = DateTimeZone.UTC;
    private static final String S3_REPO_DATE_FORMAT = "yyyyMMdd";
    private static final String indexPrefix = "test_index";
    private static final int numDays = 5;


    private static Injector injector;
    public static Client client0;

    @Mocked
    private static ESTransportClient esTransportClient;

    private static IConfiguration conf;
    @Mocked
    private static ElasticSearchIndexManager elasticSearchIndexManager;

    @BeforeClass
    public static void setup() throws InterruptedException, IOException {
        injector = Guice.createInjector(new UnitTestModule());
        conf = injector.getInstance(IConfiguration.class);

        Mockit.setUpMock(ESTransportClient.class, MockESTransportClient.class);
        esTransportClient = injector.getInstance(ESTransportClient.class);

        Mockit.setUpMock(ElasticSearchIndexManager.class, MockElasticSearchIndexManager.class);
        if(elasticSearchIndexManager == null)
            elasticSearchIndexManager = injector.getInstance(ElasticSearchIndexManager.class);

    }

    @AfterClass
    public static void cleanup() throws IOException
    {
        injector = null;
        client0 = null;
        esTransportClient = null;
        conf = null;
        elasticSearchIndexManager = null;
    }

    @Ignore
    public static class MockESTransportClient
    {
        @Mock
        public static ESTransportClient instance(IConfiguration config)
        {
            return esTransportClient;
        }

       @Mock
       public Client getTransportClient(){

           return client0;
       }
    }

    @Ignore
    public static class MockElasticSearchIndexManager
    {
        @Mock
        public IndicesStatusResponse getIndicesStatusResponse(Client esTransportClient) {
            return getLocalIndicesStatusResponse();
        }

        @Mock
        public void deleteIndices(Client client, String indexName, int timeout)
        {
            client0.admin().indices().prepareDelete(indexName).execute().actionGet(timeout);
        }
    }

    @Test
    public void testIndexRetentionWithPreCreate() throws Exception {

        client0 = client();

        Map<String, IndexStatus> beforeIndexStatusMap = getLocalIndicesStatusResponse().getIndices();
        assertEquals(0,beforeIndexStatusMap.size());

        //Create Old indices for {numDays}
        createOldIndices(indexPrefix, numDays);

        Map<String, IndexStatus> afterIndexStatusMap = getLocalIndicesStatusResponse().getIndices();
        assertEquals(numDays,afterIndexStatusMap.size());

        elasticSearchIndexManager.runIndexManagement();

        Map<String, IndexStatus> finalIndexStatusMap = getLocalIndicesStatusResponse().getIndices();

        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(conf.getIndexMetadata());

        /**
         * If Pre-Create is Enabled, it will create Today's Index + (Retention Period in Days - 1)day indices for future days
         */
        if(indexMetadataList.get(0).isPreCreate())
            assertEquals((indexMetadataList.get(0).getRetentionPeriod()-1)*2 + 1,finalIndexStatusMap.size());
        else
            assertEquals(indexMetadataList.get(0).getRetentionPeriod()-1,finalIndexStatusMap.size());

    }

    public static IndicesStatusResponse getLocalIndicesStatusResponse()
    {
        return client0.admin().indices().prepareStatus().execute().actionGet(conf.getAutoCreateIndexTimeout());
    }

    public static void createOldIndices(String indexPrefix, int numDays)
    {
        for(int i=numDays; i>0; i--) {
            String indexName = indexPrefix + getFormattedDate(i);
            client0.admin().indices().prepareCreate(indexName).execute().actionGet();
        }
    }

    public static String getFormattedDate(int priorDay)
    {
        DateTime dt = new DateTime().minusDays(priorDay).withZone(currentZone);
        DateTimeFormatter fmt = DateTimeFormat.forPattern(S3_REPO_DATE_FORMAT);
        return dt.toString(fmt);
    }
}
