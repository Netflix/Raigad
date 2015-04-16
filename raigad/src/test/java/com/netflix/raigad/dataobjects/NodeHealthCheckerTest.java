package com.netflix.raigad.dataobjects;

import com.netflix.raigad.configuration.FakeConfiguration;
import com.netflix.raigad.configuration.IConfiguration;
import org.junit.Before;
import org.junit.Test;

public class NodeHealthCheckerTest {

    private IConfiguration config;
    NodeHealthChecker nodeHealthChecker;

    @Before
    public void setup() {
        nodeHealthChecker = new NodeHealthChecker(new FakeConfiguration());
    }

    @Test
    public void testParseResponse() {
        String msg = "";
        assert nodeHealthChecker.parseResponse(msg) == 500;
        msg = "\n" +
                "{\n" +
                "  \"status\" : 200,\n" +
                "  \"name\" : \"us-west-1a.i-36be79fd\",\n" +
                "  \"cluster_name\" : \"es_alfasi2\",\n" +
                "  \"version\" : {\n" +
                "    \"number\" : \"1.4.1\",\n" +
                "    \"build_hash\" : \"89d3241d670db65f994242c8e8383b169779e2d4\",\n" +
                "    \"build_timestamp\" : \"2014-11-26T15:49:29Z\",\n" +
                "    \"build_snapshot\" : false,\n" +
                "    \"lucene_version\" : \"4.10.2\"\n" +
                "  },\n" +
                "  \"tagline\" : \"You Know, for Search\"\n" +
                "}\n";
        assert nodeHealthChecker.parseResponse(msg) == 200;
    }

}