/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.indexmanagement.exception.UnsupportedAutoIndexException;
import com.netflix.raigad.indexmanagement.indexfilters.DatePatternIndexNameFilter;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISOPeriodFormat;


public class IndexMetadata {

    private static Period[] AMOUNTS = new Period[] {
        Period.minutes(1),
        Period.hours(1),
        Period.days(1),
        Period.weeks(1),
        Period.months(1),
        Period.years(1)
    };

    public enum RETENTION_TYPE {
        HOURLY("YYYYMMddHH", "PT%dH"),
        DAILY("YYYYMMdd", "P%dD"),
        MONTHLY("YYYYMM", "P%dM"),
        YEARLY("YYYY", "P%dY");

        public final String datePattern;
        public final String periodFormat;

        RETENTION_TYPE(String datePattern, String periodFormat) {
            this.datePattern = datePattern;
            this.periodFormat = periodFormat;
        }
    }

    private final String indexNamePattern;
    private final DateTimeFormatter formatter;
    private final Period retentionPeriod;
    private final IIndexNameFilter indexNameFilter;
    private final boolean preCreate;

    @JsonCreator
    public IndexMetadata(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("indexNamePattern") String indexNamePattern,
            @JsonProperty("retentionType") String retentionType,
            @JsonProperty("retentionPeriod") String retentionPeriod,
            @JsonProperty("preCreate") Boolean preCreate) throws UnsupportedAutoIndexException {

        if (retentionType == null) {
            retentionType = "DAILY";
        }
        RETENTION_TYPE retType = RETENTION_TYPE.valueOf(retentionType.toUpperCase());

        // If legacy prefix is used, then quote it so it will be used as plain text in
        // date pattern
        String prefix = (indexName == null) ? "" : "'" + indexName + "'";

        String namePattern = (indexNamePattern == null)
            ? prefix + retType.datePattern
            : indexNamePattern;

        this.indexNamePattern = (indexName == null && indexNamePattern == null)
            ? null
            : namePattern;

        this.formatter = DateTimeFormat.forPattern(namePattern).withZoneUTC();
        this.indexNameFilter = new DatePatternIndexNameFilter(formatter);

        if (retentionPeriod == null) {
            this.retentionPeriod = null;
        } else if (retentionPeriod.startsWith("P")) {
            this.retentionPeriod = ISOPeriodFormat.standard().parsePeriod(retentionPeriod);
        } else {
            Integer num = Integer.parseInt(retentionPeriod);
            String period = String.format(retType.periodFormat, num);
            this.retentionPeriod = ISOPeriodFormat.standard().parsePeriod(period);
        }

        this.preCreate = preCreate == null ? false : preCreate;
    }

    @Override
    public String toString() {
        return String.format("{\"indexNamePattern\": \"%s\", \"retentionPeriod\": \"%s\", \"preCreate\": %b}",
                indexNamePattern, retentionPeriod, preCreate);
    }

    public String getIndexNamePattern() {
        return indexNamePattern;
    }

    public Period getRetentionPeriod() {
        return retentionPeriod;
    }

    public IIndexNameFilter getIndexNameFilter() {
        return indexNameFilter;
    }

    public boolean isPreCreate() {
        return preCreate;
    }

    public boolean isActionable() {
        return indexNamePattern != null && retentionPeriod != null;
    }

    public DateTime getPastRetentionCutoffDate(DateTime currentDateTime) {
        // After computing the cutoff we print then reparse the cutoff time to round to
        // the significant aspects of the time based on the formatter. For example:
        //
        // currentDateTime = 2018-02-03T23:47
        // retentionPeriod = P2Y
        // cutoff = 2016-02-03T23:47
        //
        // If the index pattern is yyyy, then a 2016 index would be before the cutoff so it
        // would get dropped. We want to floor the cutoff time to only the significant aspects
        // which for this example would be the year.
        DateTime cutoff = currentDateTime.minus(retentionPeriod);
        return formatter.parseDateTime(formatter.print(cutoff));
    }

    public DateTime getDateForIndexName(String name) {
        return formatter.parseDateTime(name);
    }

    public String getIndexNameToPreCreate(DateTime currentDateTime) throws UnsupportedAutoIndexException {
        String currentIndexName = formatter.print(currentDateTime);
        for (int i = 0; i < AMOUNTS.length; ++i) {
            String newIndexName = formatter.print(currentDateTime.plus(AMOUNTS[i]));
            if (!currentIndexName.equals(newIndexName)) {
                return newIndexName;
            }
        }
        throw new UnsupportedAutoIndexException("Invalid date pattern, do not know how to pre create");
    }
}
