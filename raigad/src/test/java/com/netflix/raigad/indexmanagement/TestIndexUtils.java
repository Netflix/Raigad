package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.indexmanagement.exception.UnsupportedAutoIndexException;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestIndexUtils {

    @Test
    public void testPastRetentionCutoffDate() throws IOException, UnsupportedAutoIndexException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
                "[ { \"retentionType\": \"yearly\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log\" }," +
                        "{ \"retentionType\": \"monthly\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log\" }," +
                        "{ \"retentionType\": \"hourly\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log\", \"preCreate\": \"true\" }," +
                        "{ \"retentionType\": \"hourly\", \"retentionPeriod\": 40, \"indexName\": \"nf_errors_log\", \"preCreate\": \"true\" }," +
                        "{ \"retentionType\": \"daily\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log\", \"preCreate\": \"false\" }]");

        IndexMetadata yearlyMetadata = indexMetadataList.get(0);
        IndexMetadata monthlyMetadata = indexMetadataList.get(1);
        IndexMetadata hourlyMetadata20 = indexMetadataList.get(2);
        IndexMetadata hourlyMetadata40 = indexMetadataList.get(3);
        IndexMetadata dailyMetadata = indexMetadataList.get(4);

        DateTime currentDateTime = new DateTime("2017-11-15T12:34:56Z");

        assertEquals(1997, IndexUtils.getPastRetentionCutoffDate(yearlyMetadata, currentDateTime));
        assertEquals(201603, IndexUtils.getPastRetentionCutoffDate(monthlyMetadata, currentDateTime));
        assertEquals(20171026, IndexUtils.getPastRetentionCutoffDate(dailyMetadata, currentDateTime));
        assertEquals(2017111416, IndexUtils.getPastRetentionCutoffDate(hourlyMetadata20, currentDateTime));
        assertEquals(2017111320, IndexUtils.getPastRetentionCutoffDate(hourlyMetadata40, currentDateTime));
    }

    @Test
    public void testIndexNameToPreCreate() throws IOException, UnsupportedAutoIndexException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
                "[ { \"retentionType\": \"yearly\", \"retentionPeriod\": 20, \"indexName\": \"index\" }," +
                        "{ \"retentionType\": \"monthly\", \"retentionPeriod\": 20, \"indexName\": \"0\" }," +
                        "{ \"retentionType\": \"hourly\", \"retentionPeriod\": 20, \"indexName\": \"index1\", \"preCreate\": \"true\" }," +
                        "{ \"retentionType\": \"daily\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log_useast1\", \"preCreate\": \"false\" }]");

        IndexMetadata yearlyMetadata = indexMetadataList.get(0);
        IndexMetadata monthlyMetadata = indexMetadataList.get(1);
        IndexMetadata hourlyMetadata = indexMetadataList.get(2);
        IndexMetadata dailyMetadata = indexMetadataList.get(3);

        DateTime currentDateTime = new DateTime("2017-11-15T12:34:56Z");

        assertEquals("index2018", IndexUtils.getIndexNameToPreCreate(yearlyMetadata, currentDateTime));
        assertEquals("0201712", IndexUtils.getIndexNameToPreCreate(monthlyMetadata, currentDateTime));
        assertEquals("nf_errors_log_useast120171116", IndexUtils.getIndexNameToPreCreate(dailyMetadata, currentDateTime));
        assertEquals("index12017111513", IndexUtils.getIndexNameToPreCreate(hourlyMetadata, currentDateTime));
    }
}