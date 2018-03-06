package com.netflix.raigad.indexmanagement;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.joda.time.Period;
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
        assertEquals(indexMetadataList.get(0).getRetentionPeriod().toString(), "P20M");
        assertEquals(indexMetadataList.get(0).getIndexNamePattern(), null);
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
        assertEquals(indexMetadataList.get(0).getRetentionPeriod(), null);
        assertFalse(indexMetadataList.get(0).isActionable());
    }

    @Test(expected = JsonMappingException.class)
    public void testBadInputInvalidRetention() throws IOException {
        IndexUtils.parseIndexMetadata(
                "[{\"retentionType\": \"monthly\", \"indexName\": \"nf_errors_log\",\"retentionPeriod\":\"A\"}]");
    }

    @Test(expected = JsonMappingException.class)
    public void testBadInputInvalidNamePattern() throws IOException {
        IndexUtils.parseIndexMetadata(
            "[{\"indexNamePattern\": \"nf_errors_logYYYY\",\"retentionPeriod\":\"P1M\"}]");
    }

    @Test(expected = JsonParseException.class)
    public void testBadInputBadJson() throws IOException {
        IndexUtils.parseIndexMetadata("[{\"retentionType\": \"monthly\", \"indexName\": \"nf_errors_log\",");
    }

    @Test
    public void testFiveMinuteRetention() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
            "[{\"indexNamePattern\": \"'nf_errors_log'YYYY\",\"retentionPeriod\":\"PT5M\"}]");
        IndexMetadata indexMetadata = indexMetadataList.get(0);
        assertEquals(Period.minutes(5), indexMetadata.getRetentionPeriod());
    }

    @Test
    public void testOneHourRetention() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
            "[{\"indexNamePattern\": \"'nf_errors_log'YYYY\",\"retentionPeriod\":\"PT1H\"}]");
        IndexMetadata indexMetadata = indexMetadataList.get(0);
        assertEquals(Period.hours(1), indexMetadata.getRetentionPeriod());
    }

    @Test
    public void test18MonthRetention() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
            "[{\"indexNamePattern\": \"'nf_errors_log'YYYY\",\"retentionPeriod\":\"P18M\"}]");
        IndexMetadata indexMetadata = indexMetadataList.get(0);
        assertEquals(Period.months(18), indexMetadata.getRetentionPeriod());
    }

    @Test
    public void testNamePatternOverridesRetentionType() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
            "[{\"indexNamePattern\": \"'nf_errors_log'YYYY\",\"retentionType\":\"daily\",\"retentionPeriod\":\"P18M\"}]");
        IndexMetadata indexMetadata = indexMetadataList.get(0);
        assertEquals("'nf_errors_log'YYYY", indexMetadata.getIndexNamePattern());
    }

    @Test
    public void testNamePatternOverridesIndexName() throws IOException {
        List<IndexMetadata> indexMetadataList = IndexUtils.parseIndexMetadata(
            "[{\"indexNamePattern\": \"'nf_errors_log'YYYY\",\"indexName\":\"errors\",\"retentionPeriod\":\"P18M\"}]");
        IndexMetadata indexMetadata = indexMetadataList.get(0);
        assertEquals("'nf_errors_log'YYYY", indexMetadata.getIndexNamePattern());
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
        assertEquals(indexMetadata.getIndexNamePattern(), "'nf_errors_log'YYYY");
        assertEquals(indexMetadata.getRetentionPeriod().toString(), "P20Y");
        assertTrue(indexMetadata.isActionable());

        indexMetadata = indexMetadataList.get(1);

        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log201312"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_lgg201312"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20131212"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20141233"));
        assertFalse(indexMetadata.isPreCreate());
        assertEquals(indexMetadata.getIndexNamePattern(), "'nf_errors_log'YYYYMM");
        assertEquals(indexMetadata.getRetentionPeriod().toString(), "P20M");
        assertTrue(indexMetadata.isActionable());

        indexMetadata = indexMetadataList.get(2);

        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013121201"));
        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013121200"));
        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013121223"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_lgg2013121223"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log2013121224"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20141233"));
        assertTrue(indexMetadata.isPreCreate());
        assertEquals(indexMetadata.getIndexNamePattern(), "'nf_errors_log'YYYYMMddHH");
        assertEquals(indexMetadata.getRetentionPeriod().toString(), "PT20H");
        assertTrue(indexMetadata.isActionable());

        indexMetadata = indexMetadataList.get(3);

        assertTrue(indexMetadata.getIndexNameFilter().filter("nf_errors_log20131212"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_lgg20141230"));
        assertFalse(indexMetadata.getIndexNameFilter().filter("nf_errors_log20141233"));
        assertFalse(indexMetadata.isPreCreate());
        assertEquals(indexMetadata.getIndexNamePattern(), "'nf_errors_log'YYYYMMdd");
        assertEquals(indexMetadata.getRetentionPeriod().toString(), "P20D");
        assertTrue(indexMetadata.isActionable());
    }
}
