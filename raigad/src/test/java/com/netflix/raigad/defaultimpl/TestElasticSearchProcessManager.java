package com.netflix.raigad.defaultimpl;

import com.netflix.raigad.configuration.FakeConfiguration;
import com.netflix.raigad.utils.FakeSleeper;
import com.netflix.raigad.configuration.IConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestElasticSearchProcessManager
{
    ElasticSearchProcessManager elasticSearchProcessManager;

    @Before
    public void setup()
    {
        IConfiguration config = new FakeConfiguration("us-east-1", "test_cluster", "us-east-1a", "i-1234afd3");
        elasticSearchProcessManager = new ElasticSearchProcessManager(config, new FakeSleeper());
    }

    @Test
    public void logProcessOutput_BadApp() throws IOException, InterruptedException
    {
        Process p = null;
        try
        {
            p = new ProcessBuilder("ls", "/tmppppp").start();
            int exitValue = p.waitFor();
            assertTrue(0 != exitValue);
            elasticSearchProcessManager.logProcessOutput(p);
        }
        catch(IOException ioe)
        {
            if(p!=null)
                elasticSearchProcessManager.logProcessOutput(p);
        }
    }

    /**
     * note: this will succeed on a *nix machine, unclear about anything else...
     */
    @Test
    public void logProcessOutput_GoodApp() throws IOException, InterruptedException
    {
        Process p = new ProcessBuilder("true").start();
        int exitValue = p.waitFor();
        assertEquals(0, exitValue);
        elasticSearchProcessManager.logProcessOutput(p);
    }
}
