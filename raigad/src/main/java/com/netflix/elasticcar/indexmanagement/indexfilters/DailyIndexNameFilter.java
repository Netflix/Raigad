package com.netflix.elasticcar.indexmanagement.indexfilters;

import com.netflix.elasticcar.indexmanagement.IIndexNameFilter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 * Courtesy: Jae Bae
 */
public class DailyIndexNameFilter implements IIndexNameFilter {
    public static final String id = "daily";

    @Override
    public boolean filter(String name) {
        if (name.length() < 9) {
            return false;
        }
        String date = name.substring(name.length() - 8, name.length());
        try {
            DateTime.parse(date, DateTimeFormat.forPattern("YYYYMMdd"));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getNamePart(String name) {
        return name.substring(0, name.length() - 8);
    }

    @Override
    public String getId() {
        return id;
    }
}

