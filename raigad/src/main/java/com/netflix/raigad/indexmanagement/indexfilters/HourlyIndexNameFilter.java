package com.netflix.raigad.indexmanagement.indexfilters;

import com.netflix.raigad.indexmanagement.IIndexNameFilter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class HourlyIndexNameFilter implements IIndexNameFilter {
    public static final String id = "hourly";

    @Override
    public boolean filter(String name) {
        if (name.length() < 11) {
            return false;
        }

        String date = name.substring(name.length() - 10, name.length());

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
        return id;
    }
}
