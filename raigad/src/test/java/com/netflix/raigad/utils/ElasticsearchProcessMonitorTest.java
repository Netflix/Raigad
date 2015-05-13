package com.netflix.raigad.utils;

import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import com.netflix.raigad.configuration.UnitTestModule;
import com.netflix.raigad.scheduler.ExecutionException;
import mockit.Mock;
import mockit.Mocked;
import mockit.Mockit;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class ElasticsearchProcessMonitorTest {

    @Mocked
    private ElasticsearchProcessMonitor espMonitor;
    private static Injector injector;

    @Before
    public void setup() throws IOException, ExecutionException {
        System.out.println("Running setup now ...");
        injector = Guice.createInjector(new UnitTestModule());
        Mockit.setUpMock(ElasticsearchProcessMonitor.class, MockElasticsearchProcessMonitor.class);
        espMonitor = injector.getInstance(ElasticsearchProcessMonitor.class);
        espMonitor.initialize();
    }

    @Test
    public void testAnalyzeResponseOK() throws Exception {
        JSONObject response = espMonitor.getESResponse();
        System.out.println(response.toJSONString());
        Long status = ElasticsearchProcessMonitor.analyzeResponse(response);
        assert status == 200L;
        System.out.println("testAnalyzeResponseOK() passed successfully");
    }

    @Test
    public void testAnalyzeResponseKO() throws Exception {
        JSONObject response = MockElasticsearchProcessMonitor.getKOResponse();
        System.out.println(response.toJSONString());
        Long status = ElasticsearchProcessMonitor.analyzeResponse(response);
        assert status == 500L;
        System.out.println("testAnalyzeResponseKO() passed successfully");
    }

    @Test
    public void testSetElasticsearchStarted() throws Exception {
        boolean isESRunning = ElasticsearchProcessMonitor.isElasticsearchRunning();
        assertFalse(isESRunning);
        System.out.println("Elasticsearch is not running");

        ElasticsearchProcessMonitor.setElasticsearchStarted();
        isESRunning = ElasticsearchProcessMonitor.isElasticsearchRunning();
        assertTrue(isESRunning);
        System.out.println("Elasticsearch is running");
        System.out.println("testSetElasticsearchStarted() passed successfully");
    }

//    @Test
//    public void testUpdateStateHolders() {
//        System.out.println("espMonitor:" + espMonitor);
//        boolean isESRunning = espMonitor.isElasticsearchRunning();
//        assertFalse(isESRunning);
//
//        boolean wasESRunning = espMonitor.getWasElasticsearchStarted();
//        assertFalse(wasESRunning);
//
//        espMonitor.updateStateHolders(200L);
//
//        isESRunning = espMonitor.isElasticsearchRunning();
//        assertTrue(isESRunning);
//
//        wasESRunning = espMonitor.getWasElasticsearchStarted();
//        assertTrue(wasESRunning);
//    }

    @Ignore
    static class MockElasticsearchProcessMonitor {

        @Mock
        protected JSONObject getESResponse() throws Exception {
            String RESPONSE = "{\n" +
                    "  \"status\" : 200,\n" +
                    "  \"name\" : \"us-west-1c.i-e02b4923\",\n" +
                    "  \"version\" : {\n" +
                    "    \"number\" : \"1.1.0\",\n" +
                    "    \"build_hash\" : \"2181e113dea80b4a9e31e58e9686658a2d46e363\",\n" +
                    "    \"build_timestamp\" : \"2014-03-25T15:59:51Z\",\n" +
                    "    \"build_snapshot\" : false,\n" +
                    "    \"lucene_version\" : \"4.7\"\n" +
                    "  },\n" +
                    "  \"tagline\" : \"You Know, for Search\"\n" +
                    "}\n";
            return (JSONObject) new JSONParser().parse(RESPONSE);
        }

        protected static JSONObject getKOResponse() throws Exception {
            String RESPONSE = "{\n" +
                    "  \"status\" : 500,\n" +
                    "  \"name\" : \"us-west-1c.i-e02b4923\",\n" +
                    "  \"version\" : {\n" +
                    "    \"number\" : \"1.1.0\",\n" +
                    "    \"build_hash\" : \"2181e113dea80b4a9e31e58e9686658a2d46e363\",\n" +
                    "    \"build_timestamp\" : \"2014-03-25T15:59:51Z\",\n" +
                    "    \"build_snapshot\" : false,\n" +
                    "    \"lucene_version\" : \"4.7\"\n" +
                    "  },\n" +
                    "  \"tagline\" : \"You Know, for Search\"\n" +
                    "}\n";
            return (JSONObject) new JSONParser().parse(RESPONSE);
        }
    }

    @After
    public void tearDown() {
        Mockit.tearDownMocks(ElasticsearchProcessMonitor.class);
        System.out.println("Tearing down setup ...");
    }
}