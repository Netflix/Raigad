package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.indexmanagement.indexfilters.DailyIndexNameFilter;
import com.netflix.raigad.indexmanagement.indexfilters.HourlyIndexNameFilter;
import com.netflix.raigad.indexmanagement.indexfilters.MonthlyIndexNameFilter;
import com.netflix.raigad.indexmanagement.indexfilters.YearlyIndexNameFilter;
import org.junit.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestIndexNameFilter {
    @Test
    public void testHourlyIndexNameFilter() {
        IIndexNameFilter filter = new HourlyIndexNameFilter("abcd");

        assertTrue(filter.filter("abcd2013120300"));
        assertEquals("abcd", filter.getNamePart("abcd2013120300"));

        assertTrue(filter.filter("abcd2013120301"));
        assertEquals("abcd", filter.getNamePart("abcd2013120301"));

        assertTrue(filter.filter("abcd2013120312"));
        assertEquals("abcd", filter.getNamePart("abcd2013120312"));

        assertTrue(filter.filter("abcd2013120323"));
        assertEquals("abcd", filter.getNamePart("abcd2013120323"));

        assertFalse(filter.filter("abcd12013120323"));
        assertEquals("abcd1", filter.getNamePart("abcd12013120323"));

        assertFalse(filter.filter("abcd2013120324"));
        assertFalse(filter.filter("abcd2013120345"));
        assertFalse(filter.filter("abcd20231248"));
        assertFalse(filter.filter("_abc"));
    }

    @Test
    public void testDailyIndexNameFilter() {
        IIndexNameFilter filter = new DailyIndexNameFilter("abcd");

        assertTrue(filter.filter("abcd20131203"));
        assertEquals("abcd", filter.getNamePart("abcd20131203"));

        assertFalse(filter.filter("abcd120131203"));
        assertEquals("abcd1", filter.getNamePart("abcd120131203"));

        assertFalse(filter.filter("abcd20231248"));
        assertFalse(filter.filter("abcd202312"));
        assertFalse(filter.filter("_abc"));
    }

    @Test
    public void testMonthlyIndexNameFilter() {
        IIndexNameFilter filter = new MonthlyIndexNameFilter("abcd");

        assertTrue(filter.filter("abcd202312"));
        assertEquals("abcd", filter.getNamePart("abcd202312"));

        assertFalse(filter.filter("abcd1202312"));
        assertEquals("abcd1", filter.getNamePart("abcd1202312"));

        assertFalse(filter.filter("abcd20131203"));
        assertEquals("abcd20", filter.getNamePart("abcd20131203"));

        assertFalse(filter.filter("_abc"));
        assertFalse(filter.filter("abcd20231"));
        assertFalse(filter.filter("abcd202313"));
        assertFalse(filter.filter("abcd20231248"));
    }

    @Test
    public void testYearlyIndexNameFilter() {
        IIndexNameFilter filter = new YearlyIndexNameFilter("abcd");
        assertTrue(filter.filter("abcd2023"));
        assertFalse(filter.filter("abcd20131203"));
        assertFalse(filter.filter("_abc"));
        assertFalse(filter.filter("abcd202"));
    }
}