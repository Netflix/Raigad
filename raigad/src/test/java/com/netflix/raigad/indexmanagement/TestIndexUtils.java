package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.indexmanagement.exception.UnsupportedAutoIndexException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestIndexUtils {

    private static DateTime dateTime(int v, String fmt) {
        return DateTimeFormat.forPattern(fmt).withZoneUTC().parseDateTime("" + v);
    }

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

        assertEquals(dateTime(1997, "yyyy"), yearlyMetadata.getPastRetentionCutoffDate(currentDateTime));
        assertEquals(dateTime(201603, "yyyyMM"), monthlyMetadata.getPastRetentionCutoffDate(currentDateTime));
        assertEquals(dateTime(20171026, "yyyyMMdd"), dailyMetadata.getPastRetentionCutoffDate(currentDateTime));
        assertEquals(dateTime(2017111416, "yyyyMMddHH"), hourlyMetadata20.getPastRetentionCutoffDate(currentDateTime));
        assertEquals(dateTime(2017111320, "yyyyMMddHH"), hourlyMetadata40.getPastRetentionCutoffDate(currentDateTime));
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

        assertEquals("index2018", yearlyMetadata.getIndexNameToPreCreate(currentDateTime));
        assertEquals("0201712", monthlyMetadata.getIndexNameToPreCreate(currentDateTime));
        assertEquals("nf_errors_log_useast120171116", dailyMetadata.getIndexNameToPreCreate(currentDateTime));
        assertEquals("index12017111513", hourlyMetadata.getIndexNameToPreCreate(currentDateTime));
    }
}