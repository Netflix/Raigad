package com.netflix.elasticcar.defaultimpl;

import com.google.common.io.Files;
import com.netflix.elasticcar.configuration.FakeConfiguration;
import com.netflix.elasticcar.configuration.IConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestStandardTuner
{

    private IConfiguration config;
    private StandardTuner tuner;

    @Before
    public void setup() {

        config = new FakeConfiguration();
        tuner = new StandardTuner(config);
    }

    @Test
    public void dump() throws IOException
    {
        String target = "/tmp/raigad_test.yaml";
        Files.copy(new File("src/test/resources/elasticsearch.yml"), new File("/tmp/raigad_test.yaml"));
        tuner.writeAllProperties(target, "your_host");
    }
}