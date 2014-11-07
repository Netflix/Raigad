package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.indexmanagement.exception.UnsupportedAutoIndexException;
import org.joda.time.DateTime;
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
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(str);
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
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(str);
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
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(str);
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
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(str);
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
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(str);
        assertEquals(indexMetadataList.size(), 1);
        assertTrue(indexMetadataList.get(0).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("a20141233"));
        assertTrue(indexMetadataList.get(0).isPreCreate());
    }

    @Test
    public void testBackupRepo() throws IOException
    {
        String str = "[    {        \"retentionType\": \"daily\",        \"retentionPeriod\": 20,    \"indexName\": \"nf_errors_log\"     }]";
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(str);
        assertEquals(indexMetadataList.size(), 1);

    }

    @Test
    public void testIndexCreation() throws Exception {
        String str = "[   {        \"retentionType\": \"daily\",        \"retentionPeriod\": 3,    \"indexName\": \"dailyindex\", \"preCreate\": \"true\"     }," +
                "{        \"retentionType\": \"daily\",        \"retentionPeriod\": 3,    \"indexName\": \"dailyindex2\", \"preCreate\": \"true\"     }," +
                "{        \"retentionType\": \"monthly\",        \"retentionPeriod\": 3,    \"indexName\": \"monthlyindex\", \"preCreate\": \"true\"     }," +
                "{        \"retentionType\": \"yearly\",        \"retentionPeriod\": 3,    \"indexName\": \"yearlyindex\", \"preCreate\": \"true\"     }]";
        List<IndexMetadata> indexMetadataList = ElasticSearchIndexManager.buildInfo(str);
        for (IndexMetadata indexMetadata : indexMetadataList) {
            System.out.println("Retention Period : " + indexMetadata.getRetentionPeriod());
            for (int i = 0; i < indexMetadata.getRetentionPeriod(); ++i) {
                DateTime dt = new DateTime();
                int addedDate;

                switch (indexMetadata.getRetentionType()) {
                    case DAILY:
                        dt = dt.plusDays(i);
                        addedDate = Integer.parseInt(String.format("%d%02d%02d", dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth()));
                        break;
                    case MONTHLY:
                        dt = dt.plusMonths(i);
                        addedDate = Integer.parseInt(String.format("%d%02d", dt.getYear(), dt.getMonthOfYear()));
                        break;
                    case YEARLY:
                        dt = dt.plusYears(i);
                        addedDate = Integer.parseInt(String.format("%d", dt.getYear()));
                        break;
                    default:
                        throw new UnsupportedAutoIndexException("Given index is not (DAILY or MONTHLY or YEARLY), please check your configuration.");

                }
                System.out.println("Date to add : " + addedDate);
                System.out.println("New Index Name : " + indexMetadata.getIndexName() + addedDate);
            }
        }
    }
}
