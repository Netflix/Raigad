package com.netflix.raigad.utils;

import com.netflix.raigad.configuration.FakeConfiguration;
import mockit.*;
import mockit.integration.junit4.JMockit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@RunWith(JMockit.class)
public class TestElasticsearchProcessMonitor {
    private static String ELASTICSEARCH_PROCESS_NAME = FakeConfiguration.ES_PROCESS_NAME;

    @After
    public void resetElasticsearchProcessMonitor() {
        ElasticsearchProcessMonitor.isElasticsearchRunningNow.set(false);
        ElasticsearchProcessMonitor.wasElasticsearchStarted.set(false);
    }

    @Test
    public void testNullInputStream(
            @Mocked Runtime runtime,
            @Mocked Process pgrepProcess,
            @Mocked InputStreamReader inputStreamReader,
            @Mocked BufferedReader bufferedReader) throws Exception {
        new Expectations() {
            {
                pgrepProcess.getInputStream();
                times = 1;

                bufferedReader.readLine();
                result = null;
                times = 1;
            }
        };

        new MockUp<Runtime>() {
            @Mock
            Runtime getRuntime() {
                return runtime;
            }

            @Mock
            Process exec(String command) {
                return pgrepProcess;
            }
        };

        ElasticsearchProcessMonitor.checkElasticsearchProcess(ELASTICSEARCH_PROCESS_NAME);

        new VerificationsInOrder() {
            {
                bufferedReader.close();
                inputStreamReader.close();
                pgrepProcess.destroyForcibly();
            }
        };

        Assert.assertFalse(ElasticsearchProcessMonitor.isElasticsearchRunning());
        Assert.assertFalse(ElasticsearchProcessMonitor.getWasElasticsearchStarted());
    }

    @Test
    public void testEmptyInputStream(
            @Mocked Runtime runtime,
            @Mocked Process pgrepProcess,
            @Mocked InputStreamReader inputStreamReader,
            @Mocked BufferedReader bufferedReader) throws Exception {
        new Expectations() {
            {
                pgrepProcess.getInputStream();
                times = 1;

                bufferedReader.readLine();
                result = "\n";
                times = 1;
            }
        };

        new MockUp<Runtime>() {
            @Mock
            Runtime getRuntime() {
                return runtime;
            }

            @Mock
            Process exec(String command) {
                return pgrepProcess;
            }
        };

        ElasticsearchProcessMonitor.checkElasticsearchProcess(ELASTICSEARCH_PROCESS_NAME);

        new VerificationsInOrder() {
            {
                bufferedReader.close();
                inputStreamReader.close();
                pgrepProcess.destroyForcibly();
            }
        };

        Assert.assertFalse(ElasticsearchProcessMonitor.isElasticsearchRunning());
        Assert.assertFalse(ElasticsearchProcessMonitor.getWasElasticsearchStarted());
    }

    @Test
    public void testValidInputStream(
            @Mocked Runtime runtime,
            @Mocked Process pgrepProcess,
            @Mocked InputStreamReader inputStreamReader,
            @Mocked BufferedReader bufferedReader) throws Exception {
        new Expectations() {
            {
                pgrepProcess.getInputStream();
                times = 1;

                bufferedReader.readLine();
                result = "1234";
                times = 1;
            }
        };

        new MockUp<Runtime>() {
            @Mock
            Runtime getRuntime() {
                return runtime;
            }

            @Mock
            Process exec(String command) {
                return pgrepProcess;
            }
        };

        ElasticsearchProcessMonitor.checkElasticsearchProcess(ELASTICSEARCH_PROCESS_NAME);

        new VerificationsInOrder() {
            {
                bufferedReader.close();
                inputStreamReader.close();
                pgrepProcess.destroyForcibly();
            }
        };

        Assert.assertTrue(ElasticsearchProcessMonitor.isElasticsearchRunning());
        Assert.assertTrue(ElasticsearchProcessMonitor.getWasElasticsearchStarted());
    }

    @Test
    public void testElasticsearchWasStarted(
            @Mocked Runtime runtime,
            @Mocked Process pgrepProcess,
            @Mocked InputStreamReader inputStreamReader,
            @Mocked BufferedReader bufferedReader) throws Exception {

        new Expectations() {
            {
                pgrepProcess.getInputStream();
                times = 1;

                bufferedReader.readLine();
                result = "\n";
                times = 1;
            }
        };

        new MockUp<Runtime>() {
            @Mock
            Runtime getRuntime() {
                return runtime;
            }

            @Mock
            Process exec(String command) {
                return pgrepProcess;
            }
        };

        ElasticsearchProcessMonitor.isElasticsearchRunningNow.set(true);
        ElasticsearchProcessMonitor.wasElasticsearchStarted.set(true);

        ElasticsearchProcessMonitor.checkElasticsearchProcess(ELASTICSEARCH_PROCESS_NAME);

        new VerificationsInOrder() {
            {
                bufferedReader.close();
                inputStreamReader.close();
                pgrepProcess.destroyForcibly();
            }
        };

        Assert.assertFalse(ElasticsearchProcessMonitor.isElasticsearchRunning());
        Assert.assertTrue(ElasticsearchProcessMonitor.getWasElasticsearchStarted());
    }
}
