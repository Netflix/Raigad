package com.netflix.elasticcar.indexmanagement;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by sloke on 6/27/14.
 */
public class TestIndexMetadata {

    @Test
    public void testDailyRetention() throws IOException {
        String str = "[    {        \"retentionType\": \"daily\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\"     }]";
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.IndexAllocator.buildInfo(str);
        assertEquals(indexMetadataList.size(), 1);
        assertTrue(indexMetadataList.get(0).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("a20141233"));
        assertFalse(indexMetadataList.get(0).isPreCreate());
        assertEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.DAILY);
        assertNotEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertNotEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.YEARLY);
        assertEquals(indexMetadataList.get(0).getRetentionPeriod(), 20);
    }

    @Test
    public void testMonthlyRetention() throws IOException {
        String str = "[    {        \"retentionType\": \"monthly\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\"     }]";
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.IndexAllocator.buildInfo(str);
        assertEquals(indexMetadataList.size(), 1);
        assertTrue(indexMetadataList.get(0).getIndexNameFilter().filter("index201312"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("a20141233"));
        assertFalse(indexMetadataList.get(0).isPreCreate());
        assertEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertNotEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.YEARLY);
        assertNotEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.DAILY);
        assertEquals(indexMetadataList.get(0).getRetentionPeriod(), 20);
    }

    @Test
    public void testYearlyRetention() throws IOException {
        String str = "[    {        \"retentionType\": \"yearly\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\"     }]";
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.IndexAllocator.buildInfo(str);
        assertEquals(indexMetadataList.size(), 1);
        assertTrue(indexMetadataList.get(0).getIndexNameFilter().filter("index2013"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("index201312"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("a20141233"));
        assertFalse(indexMetadataList.get(0).isPreCreate());
        assertEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.YEARLY);
        assertNotEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertNotEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.DAILY);
        assertEquals(indexMetadataList.get(0).getRetentionPeriod(), 20);
    }

    @Test
    public void testMixedRetention() throws IOException {
        String str = "[   {        \"retentionType\": \"yearly\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\"     }," +
                         "{        \"retentionType\": \"monthly\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\"     }," +
                         "{        \"retentionType\": \"daily\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\"     }]";
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.IndexAllocator.buildInfo(str);
        assertEquals(indexMetadataList.size(), 3);
        assertTrue(indexMetadataList.get(0).getIndexNameFilter().filter("index2013"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("index201312"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("a20141233"));
        assertFalse(indexMetadataList.get(0).isPreCreate());
        assertEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.YEARLY);
        assertNotEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertNotEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.DAILY);
        assertEquals(indexMetadataList.get(0).getRetentionPeriod(), 20);

        assertTrue(indexMetadataList.get(1).getIndexNameFilter().filter("index201312"));
        assertFalse(indexMetadataList.get(1).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(1).getIndexNameFilter().filter("a20141233"));
        assertFalse(indexMetadataList.get(1).isPreCreate());
        assertEquals(indexMetadataList.get(1).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertNotEquals(indexMetadataList.get(1).getRetentionType(), IndexMetadata.RETENTION_TYPE.YEARLY);
        assertNotEquals(indexMetadataList.get(1).getRetentionType(), IndexMetadata.RETENTION_TYPE.DAILY);
        assertEquals(indexMetadataList.get(1).getRetentionPeriod(), 20);

        assertTrue(indexMetadataList.get(2).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(2).getIndexNameFilter().filter("a20141233"));
        assertFalse(indexMetadataList.get(2).isPreCreate());
        assertEquals(indexMetadataList.get(2).getRetentionType(), IndexMetadata.RETENTION_TYPE.DAILY);
        assertNotEquals(indexMetadataList.get(2).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertNotEquals(indexMetadataList.get(2).getRetentionType(), IndexMetadata.RETENTION_TYPE.YEARLY);
        assertEquals(indexMetadataList.get(2).getRetentionPeriod(), 20);

    }

    @Test
    public void testPreCreate() throws IOException {
        String str = "[    {        \"retentionType\": \"daily\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\", \"preCreate\": \"true\"     }]";
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.IndexAllocator.buildInfo(str);
        assertEquals(indexMetadataList.size(), 1);
        assertTrue(indexMetadataList.get(0).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("a20141233"));
        assertTrue(indexMetadataList.get(0).isPreCreate());
    }

    @Test
    public void testBackupRepo() throws IOException
    {
        String str = "[    {        \"retentionType\": \"daily\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\"     }]";
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.IndexAllocator.buildInfo(str);
        assertEquals(indexMetadataList.size(), 1);

    }
}
