package com.netflix.raigad.indexmanagement;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TestIndexMetadata {
    @Test
    public void testBadInputNoIndexName() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
                "[{\"retentionType\": \"monthly\",\"retentionPeriod\": 20}]");

        assertEquals(indexMetadataList.size(), 1);
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("index201312"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("index20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("a20141233"));
        assertFalse(indexMetadataList.get(0).isPreCreate());
        assertEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertFalse(indexMetadataList.get(0).getRetentionType() == IndexMetadata.RETENTION_TYPE.YEARLY);
        assertFalse(indexMetadataList.get(0).getRetentionType() == IndexMetadata.RETENTION_TYPE.DAILY);
        assertEquals(indexMetadataList.get(0).getRetentionPeriod().longValue(), 20);
        assertEquals(indexMetadataList.get(0).getIndexName(), null);
        assertFalse(indexMetadataList.get(0).isActionable());
    }

    @Test
    public void testBadInputNoRetention() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
                "[{\"retentionType\": \"monthly\", \"indexName\": \"nf_errors_log\"}]");

        assertEquals(indexMetadataList.size(), 1);
        assertTrue(indexMetadataList.get(0).getIndexNameFilter().filter("nf_errors_log201312"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("nf_errors_log20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("nf_errors_log20141233"));
        assertFalse(indexMetadataList.get(0).isPreCreate());
        assertEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertFalse(indexMetadataList.get(0).getRetentionType() == IndexMetadata.RETENTION_TYPE.YEARLY);
        assertFalse(indexMetadataList.get(0).getRetentionType() == IndexMetadata.RETENTION_TYPE.DAILY);
        assertEquals(indexMetadataList.get(0).getRetentionPeriod(), null);
        assertFalse(indexMetadataList.get(0).isActionable());
    }

    @Test
    public void testBadInputInvalidSymbols() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
                "[{\"retentionType\":\"monthly\",\"indexName\":\"nf_errors_log\",\"retentionPeriod?:6,?preCreate\":false}]");

        assertEquals(indexMetadataList.size(), 1);
        assertTrue(indexMetadataList.get(0).getIndexNameFilter().filter("nf_errors_log201312"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("nf_errors_log20131212"));
        assertFalse(indexMetadataList.get(0).getIndexNameFilter().filter("nf_errors_log20141233"));
        assertFalse(indexMetadataList.get(0).isPreCreate());
        assertEquals(indexMetadataList.get(0).getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertFalse(indexMetadataList.get(0).getRetentionType() == IndexMetadata.RETENTION_TYPE.YEARLY);
        assertFalse(indexMetadataList.get(0).getRetentionType() == IndexMetadata.RETENTION_TYPE.DAILY);
        assertEquals(indexMetadataList.get(0).getRetentionPeriod(), null);
        assertFalse(indexMetadataList.get(0).isActionable());
    }

    @Test(expected = JsonMappingException.class)
    public void testBadInputInvalidRetention() throws IOException {
        IndexUtils.parseIndexMetadata(
                "[{\"retentionType\": \"monthly\", \"indexName\": \"nf_errors_log\",\"retentionPeriod\":\"A\"}]");
    }

    @Test(expected = JsonParseException.class)
    public void testBadInputBadJson() throws IOException {
        IndexUtils.parseIndexMetadata("[{\"retentionType\": \"monthly\", \"indexName\": \"nf_errors_log\",");
    }

    @Test
    public void testMixedRetention() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
                "[ { \"retentionType\": \"yearly\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log\" }," +
                        "{ \"retentionType\": \"monthly\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log\" }," +
                        "{ \"retentionType\": \"hourly\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log\", \"preCreate\": \"true\" }," +
                        "{ \"retentionType\": \"daily\", \"retentionPeriod\": 20, \"indexName\": \"nf_errors_log\", \"preCreate\": \"false\" }]");

        assertEquals(indexMetadataList.size(), 4);

        IndexMetadata indexMetadata = indexMetadataList.get(0);

        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_log2013"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log201312"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20131212"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20141233"));
        assertFalse(indexMetadata.isPreCreate());
        assertEquals(indexMetadata.getIndexName(), "nf_errors_log");
        assertEquals(indexMetadata.getRetentionType(), IndexMetadata.RETENTION_TYPE.YEARLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.DAILY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.HOURLY);
        assertEquals(indexMetadata.getRetentionPeriod().longValue(), 20);
        assertTrue(indexMetadata.isActionable());

        indexMetadata = indexMetadataList.get(1);

        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log201312"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_lgg201312"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20131212"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20141233"));
        assertFalse(indexMetadata.isPreCreate());
        assertEquals(indexMetadata.getIndexName(), "nf_errors_log");
        assertEquals(indexMetadata.getRetentionType(), IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.YEARLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.DAILY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.HOURLY);
        assertEquals(indexMetadata.getRetentionPeriod().longValue(), 20);
        assertTrue(indexMetadata.isActionable());

        indexMetadata = indexMetadataList.get(2);

        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013121201"));
        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013121200"));
        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013121223"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_lgg2013121223"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013121224"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20141233"));
        assertTrue(indexMetadata.isPreCreate());
        assertEquals(indexMetadata.getIndexName(), "nf_errors_log");
        assertEquals(indexMetadata.getRetentionType(), IndexMetadata.RETENTION_TYPE.HOURLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.YEARLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.DAILY);
        assertEquals(indexMetadata.getRetentionPeriod().longValue(), 20);
        assertTrue(indexMetadata.isActionable());

        indexMetadata = indexMetadataList.get(3);

        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log20131212"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_lgg20141230"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20141233"));
        assertFalse(indexMetadata.isPreCreate());
        assertEquals(indexMetadata.getIndexName(), "nf_errors_log");
        assertEquals(indexMetadata.getRetentionType(), IndexMetadata.RETENTION_TYPE.DAILY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.MONTHLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.YEARLY);
        assertFalse(indexMetadata.getRetentionType() == IndexMetadata.RETENTION_TYPE.HOURLY);
        assertEquals(indexMetadata.getRetentionPeriod().longValue(), 20);
        assertTrue(indexMetadata.isActionable());
    }
}