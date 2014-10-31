package com.netflix.elasticcar;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPropertiesConfigSource
{
    @Test
    public void readFile()
    {
        PropertiesConfigSource configSource = new PropertiesConfigSource("conf/escar.properties");
        configSource.initialize("asgName", "region");

        assertEquals("\"/tmp/data\"", configSource.get("escar.path.data"));
        assertEquals(9001, configSource.get("escar.transport.tcp.port", 0));
        // File has 5 lines, but line 6 is "escar.http.port9002", so it gets filtered out with empty string check.
        assertEquals(4, configSource.size());
    }

    @Test
    public void updateKey()
    {
        PropertiesConfigSource configSource = new PropertiesConfigSource("conf/escar.properties");
        configSource.initialize("asgName", "region");

        // File has 5 lines, but line 2 is "escar.http.port9002", so it gets filtered out with empty string check.
        assertEquals(4, configSource.size());

        configSource.set("foo", "bar");

        assertEquals(5, configSource.size());

        assertEquals("bar", configSource.get("foo"));

        assertEquals(9001, configSource.get("escar.transport.tcp.port", 0));
        configSource.set("escar.transport.tcp.port", Integer.toString(10));
        assertEquals(10, configSource.get("escar.transport.tcp.port", 0));
    }
}
