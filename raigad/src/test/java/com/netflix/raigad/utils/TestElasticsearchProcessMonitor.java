package com.netflix.raigad.utils;

import com.netflix.raigad.configuration.FakeConfiguration;
import com.netflix.raigad.configuration.IConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class TestElasticsearchProcessMonitor {
    private static String ELASTICSEARCH_PROCESS_NAME = FakeConfiguration.ES_PROCESS_NAME;

    private Process pgrepProcess;
    private InputStream processInputStream;

    private ElasticsearchProcessMonitor elasticsearchProcessMonitor;

    @Before
    public void setUp() throws IOException {
        processInputStream = mock(InputStream.class);

        pgrepProcess = mock(Process.class);
        when(pgrepProcess.getInputStream()).thenReturn(processInputStream);

        Runtime runtime = mock(Runtime.class);
        when(runtime.exec(anyString())).thenReturn(pgrepProcess);

        elasticsearchProcessMonitor = spy(new ElasticsearchProcessMonitor(mock(IConfiguration.class)));
        doReturn(runtime).when(elasticsearchProcessMonitor).getRuntime();
    }

    @After
    public void cleanUp() throws Exception {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(
                new ObjectName("com.netflix.raigad.scheduler:type=" + ElasticsearchProcessMonitor.class.getName()));

        ElasticsearchProcessMonitor.isElasticsearchRunningNow.set(false);
        ElasticsearchProcessMonitor.wasElasticsearchStarted.set(false);
    }

    @Test
    public void testNullInputStream() throws Exception {
        doReturn(null).when(elasticsearchProcessMonitor).getFirstLine(processInputStream);

        elasticsearchProcessMonitor.checkElasticsearchProcess(ELASTICSEARCH_PROCESS_NAME);

        verify(processInputStream, times(1)).close();
        verify(pgrepProcess, times(1)).destroyForcibly();

        Assert.assertFalse(ElasticsearchProcessMonitor.isElasticsearchRunning());
        Assert.assertFalse(ElasticsearchProcessMonitor.getWasElasticsearchStarted());
    }

    @Test
    public void testEmptyInputStream() throws Exception {
        doReturn("").when(elasticsearchProcessMonitor).getFirstLine(processInputStream);

        elasticsearchProcessMonitor.checkElasticsearchProcess(ELASTICSEARCH_PROCESS_NAME);

        verify(processInputStream, times(1)).close();
        verify(pgrepProcess, times(1)).destroyForcibly();

        Assert.assertFalse(ElasticsearchProcessMonitor.isElasticsearchRunning());
        Assert.assertFalse(ElasticsearchProcessMonitor.getWasElasticsearchStarted());
    }

    @Test
    public void testValidInputStream() throws Exception {
        doReturn("1234").when(elasticsearchProcessMonitor).getFirstLine(processInputStream);

        elasticsearchProcessMonitor.checkElasticsearchProcess(ELASTICSEARCH_PROCESS_NAME);

        verify(processInputStream, times(1)).close();
        verify(pgrepProcess, times(1)).destroyForcibly();

        Assert.assertTrue(ElasticsearchProcessMonitor.isElasticsearchRunning());
        Assert.assertTrue(ElasticsearchProcessMonitor.getWasElasticsearchStarted());
    }

    @Test
    public void testElasticsearchWasStarted() throws Exception {
        doReturn("").when(elasticsearchProcessMonitor).getFirstLine(processInputStream);

        ElasticsearchProcessMonitor.isElasticsearchRunningNow.set(true);
        ElasticsearchProcessMonitor.wasElasticsearchStarted.set(true);

        elasticsearchProcessMonitor.checkElasticsearchProcess(ELASTICSEARCH_PROCESS_NAME);

        verify(processInputStream, times(1)).close();
        verify(pgrepProcess, times(1)).destroyForcibly();

        Assert.assertFalse(ElasticsearchProcessMonitor.isElasticsearchRunning());
        Assert.assertTrue(ElasticsearchProcessMonitor.getWasElasticsearchStarted());
    }
}
