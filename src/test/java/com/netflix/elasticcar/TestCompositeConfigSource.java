package com.netflix.elasticcar;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestCompositeConfigSource
{
    @Test
    public void read()
    {
        MemoryConfigSource memoryConfigSource = new MemoryConfigSource();
        IConfigSource configSource = new CompositeConfigSource(memoryConfigSource);
        configSource.initialize("foo", "bar");

        assertEquals(0, configSource.size());
        configSource.set("foo", "bar");
        assertEquals(1, configSource.size());
        assertEquals("bar", configSource.get("foo"));

        // verify that the writes went to mem source.
        assertEquals(1, memoryConfigSource.size());
        assertEquals("bar", memoryConfigSource.get("foo"));
    }

    @Test
    public void readMultiple()
    {
        MemoryConfigSource m1 = new MemoryConfigSource();
        m1.set("foo", "foo");
        MemoryConfigSource m2 = new MemoryConfigSource();
        m2.set("bar", "bar");
        MemoryConfigSource m3 = new MemoryConfigSource();
        m3.set("baz", "baz");

        IConfigSource configSource = new CompositeConfigSource(m1, m2, m3);
        assertEquals(3, configSource.size());
        assertEquals("foo", configSource.get("foo"));
        assertEquals("bar", configSource.get("bar"));
        assertEquals("baz", configSource.get("baz"));

        // read default
        assertEquals("test", configSource.get("doesnotexist", "test"));
    }
}
