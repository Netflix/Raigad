package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.indexmanagement.indexfilters.DatePatternIndexNameFilter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestIndexNameFilter {

    @Test
    public void testWrongPrefix() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYY");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertFalse(filter.filter("foo2018"));
    }

    @Test
    public void testYearlyPattern() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYY");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd2018"));
    }

    @Test
    public void testYearlyPatternWithYYYYMM() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYY");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertFalse(filter.filter("abcd201802"));
    }

    @Test
    public void testMonthlyPattern() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMM");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd201802"));
    }

    @Test
    public void testMonthlyPatternWithSingleDigitMonth() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMM");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertFalse(filter.filter("abcd20182"));
    }

    @Test
    public void testMonthlyPatternWithYYYYMMdd() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMM");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertFalse(filter.filter("abcd20180203"));
    }

    @Test
    public void testWeeklyPattern() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'-YYYY-ww");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd-2018-51"));
    }

    @Test
    public void testWeeklyPatternInvalid() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'-YYYY-ww");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertFalse(filter.filter("abcd-2018-53"));
    }

    @Test
    public void testDailyPattern() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMMdd");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd20180203"));
    }

    @Test
    public void testHalfDayPattern() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'-YYYY-MM-dd-aa");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd-2018-02-03-AM"));
        assertTrue(filter.filter("abcd-2018-02-03-PM"));
        assertFalse(filter.filter("abcd-2018-02-03-BC"));
    }

    @Test
    public void testHourlyPattern() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMMddHH");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd2018020323"));
    }

    @Test
    public void testHourlyPatternInvalidHour() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMMddHH");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertFalse(filter.filter("abcd2018020328"));
    }

    @Test
    public void testPatternWithDashes() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYY-MM-dd");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd2018-02-27"));
    }

    @Test
    public void testPatternWithDots() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYY.MM.dd");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd2018.02.27"));
    }

    @Test
    public void testPatternWithSuffix() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYY-MM-dd'ghi'");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd2018-02-27ghi"));
    }

    @Test
    public void testHourlyIndexNameFilter() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMMddHH");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);

        assertTrue(filter.filter("abcd2013120300"));

        assertTrue(filter.filter("abcd2013120301"));

        assertTrue(filter.filter("abcd2013120312"));

        assertTrue(filter.filter("abcd2013120323"));

        assertFalse(filter.filter("abcd12013120323"));

        assertFalse(filter.filter("abcd2013120324"));
        assertFalse(filter.filter("abcd2013120345"));
        assertFalse(filter.filter("abcd20231248"));
        assertFalse(filter.filter("_abc"));
    }

    @Test
    public void testDailyIndexNameFilter() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMMdd");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);

        assertTrue(filter.filter("abcd20131203"));

        assertFalse(filter.filter("abcd120131203"));

        assertFalse(filter.filter("abcd20231248"));
        assertFalse(filter.filter("abcd202312"));
        assertFalse(filter.filter("_abc"));
    }

    @Test
    public void testMonthlyIndexNameFilter() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYYMM");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);

        assertTrue(filter.filter("abcd202312"));

        assertFalse(filter.filter("abcd1202312"));

        assertFalse(filter.filter("abcd20131203"));

        assertFalse(filter.filter("_abc"));
        System.out.println(formatter.parseDateTime("abcd20231"));
        assertFalse(filter.filter("abcd20231"));
        assertFalse(filter.filter("abcd202313"));
        assertFalse(filter.filter("abcd20231248"));
    }

    @Test
    public void testYearlyIndexNameFilter() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("'abcd'YYYY");
        IIndexNameFilter filter = new DatePatternIndexNameFilter(formatter);
        assertTrue(filter.filter("abcd2023"));
        assertFalse(filter.filter("abcd20131203"));
        assertFalse(filter.filter("_abc"));
        assertFalse(filter.filter("abcd202"));
    }
}
