package com.netflix.raigad.configuration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPropertiesConfigSource
{
    @Test
    public void readFile()
    {
        PropertiesConfigSource configSource = new PropertiesConfigSource("conf/raigad.properties");
        configSource.initialize("asgName", "region");

        assertEquals("\"/tmp/data\"", configSource.get("Raigad.path.data"));
        assertEquals(9001, configSource.get("Raigad.transport.tcp.port", 0));
        // File has 5 lines, but line 6 is "Raigad.http.port9002", so it gets filtered out with empty string check.
        assertEquals(4, configSource.size());
    }

    @Test
    public void updateKey()
    {
        PropertiesConfigSource configSource = new PropertiesConfigSource("conf/raigad.properties");
        configSource.initialize("asgName", "region");

        // File has 5 lines, but line 2 is "escar.http.port9002", so it gets filtered out with empty string check.
        assertEquals(4, configSource.size());

        configSource.set("foo", "bar");

        assertEquals(5, configSource.size());

        assertEquals("bar", configSource.get("foo"));

        assertEquals(9001, configSource.get("Raigad.transport.tcp.port", 0));
        configSource.set("Raigad.transport.tcp.port", Integer.toString(10));
        assertEquals(10, configSource.get("Raigad.transport.tcp.port", 0));
    }
}
