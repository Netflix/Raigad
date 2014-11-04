package com.netflix.elasticcar.indexmanagement;

import com.netflix.elasticcar.indexmanagement.indexfilters.DailyIndexNameFilter;
import com.netflix.elasticcar.indexmanagement.indexfilters.MonthlyIndexNameFilter;
import com.netflix.elasticcar.indexmanagement.indexfilters.YearlyIndexNameFilter;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by sloke on 6/27/14.
 */
public class TestIndexNameFilter {
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
