package com.netflix.raigad.indexmanagement.indexfilters;

import com.netflix.raigad.indexmanagement.IIndexNameFilter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.regex.Pattern;

public class HourlyIndexNameFilter implements IIndexNameFilter {
    public static final String ID = "hourly";
    private Pattern indexNamePattern;

    public HourlyIndexNameFilter(String indexNamePrefix) {
        indexNamePattern = Pattern.compile(String.format("^%s\\d{10}$", indexNamePrefix));
    }

    @Override
    public boolean filter(String indexName) {
        if (!indexNamePattern.matcher(indexName).matches()) {
            return false;
        }

        String date = indexName.substring(indexName.length() - 10, indexName.length());

        try {
            DateTime.parse(date, DateTimeFormat.forPattern("YYYYMMddHH"));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getNamePart(String name) {
        return name.substring(0, name.length() - 10);
    }

    @Override
    public String getId() {
        return ID;
    }
}
