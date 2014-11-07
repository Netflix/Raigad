package com.netflix.raigad.configuration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestSystemPropertiesConfigSource
{
    @Test
    public void read()
    {
        final String key = "java.version";
        SystemPropertiesConfigSource configSource = new SystemPropertiesConfigSource();
        configSource.initialize("asgName", "region");

        // sys props are filtered to starting with escar, so this should be missing.
        assertEquals(null, configSource.get(key));

        assertEquals(0, configSource.size());
    }
}
