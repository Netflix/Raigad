package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.indexmanagement.indexfilters.DailyIndexNameFilter;
import com.netflix.raigad.indexmanagement.indexfilters.HourlyIndexNameFilter;
import com.netflix.raigad.indexmanagement.indexfilters.MonthlyIndexNameFilter;
import com.netflix.raigad.indexmanagement.indexfilters.YearlyIndexNameFilter;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestIndexNameFilter {
    @Test
    public void testHourlyIndexNameFilter() {
        IIndexNameFilter filter = new HourlyIndexNameFilter();
        assertTrue(filter.filter("abcd2013120300"));
        assertTrue(filter.filter("abcd2013120301"));
        assertTrue(filter.filter("abcd2013120312"));
        assertTrue(filter.filter("abcd2013120323"));
        assertFalse(filter.filter("abcd2013120324"));
        assertFalse(filter.filter("abcd2013120345"));
        assertFalse(filter.filter("abcd20231248"));
        assertFalse(filter.filter("_abc"));
    }

    @Test
    public void testDailyIndexNameFilter() {
        IIndexNameFilter filter = new DailyIndexNameFilter();
        assertTrue(filter.filter("abcd20131203"));
        assertFalse(filter.filter("abcd20231248"));
        assertFalse(filter.filter("_abc"));
    }

    @Test
    public void testMonthlyIndexNameFilter() {
        IIndexNameFilter filter = new MonthlyIndexNameFilter();
        assertFalse(filter.filter("abcd20131203"));
        assertFalse(filter.filter("abcd20231248"));
        assertFalse(filter.filter("_abc"));
        assertTrue(filter.filter("abcd202312"));
        assertFalse(filter.filter("abcd202313"));
    }

    @Test
    public void testYearlyIndexNameFilter() {
        IIndexNameFilter filter = new YearlyIndexNameFilter();
        assertTrue(filter.filter("abcd2023"));
        assertFalse(filter.filter("abcd20131203"));
        assertFalse(filter.filter("abcd20231248"));
        assertFalse(filter.filter("_abc"));
        assertFalse(filter.filter("abcd202312"));
    }
}
