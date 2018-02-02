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
import com.netflix.raigad.objectmapper.DefaultIndexMapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.List;

public class IndexUtils {
    private static DateTimeFormatter hourlyDateTimeFormatter = DateTimeFormat.forPattern("YYYYMMddHH").withZoneUTC();
    private static DateTimeFormatter dailyDateTimeFormatter = DateTimeFormat.forPattern("YYYYMMdd").withZoneUTC();
    private static DateTimeFormatter monthlyDateTimeFormatter = DateTimeFormat.forPattern("YYYYMM").withZoneUTC();
    private static DateTimeFormatter yearlyDateTimeFormatter = DateTimeFormat.forPattern("YYYY").withZoneUTC();

    public static int getPastRetentionCutoffDate(IndexMetadata indexMetadata, DateTime currentDateTime) throws UnsupportedAutoIndexException {
        DateTime dateTime;

        switch (indexMetadata.getRetentionType()) {
            case HOURLY:
                dateTime = currentDateTime.minusHours(indexMetadata.getRetentionPeriod());
                return Integer.parseInt(hourlyDateTimeFormatter.print(dateTime));

            case DAILY:
                dateTime = currentDateTime.minusDays(indexMetadata.getRetentionPeriod());
                return Integer.parseInt(dailyDateTimeFormatter.print(dateTime));

            case MONTHLY:
                dateTime = currentDateTime.minusMonths(indexMetadata.getRetentionPeriod());
                return Integer.parseInt(monthlyDateTimeFormatter.print(dateTime));

            case YEARLY:
                dateTime = currentDateTime.minusYears(indexMetadata.getRetentionPeriod());
                return Integer.parseInt(yearlyDateTimeFormatter.print(dateTime));

            default:
                throw new UnsupportedAutoIndexException("Unsupported or invalid retention type (HOURLY, DAILY, MONTHLY, or YEARLY)");
        }
    }

    public static String getIndexNameToPreCreate(IndexMetadata indexMetadata, DateTime currentDateTime) throws UnsupportedAutoIndexException {
        switch (indexMetadata.getRetentionType()) {
            case HOURLY:
                return indexMetadata.getIndexName() + hourlyDateTimeFormatter.print(currentDateTime.plusHours(1));

            case DAILY:
                return indexMetadata.getIndexName() + dailyDateTimeFormatter.print(currentDateTime.plusDays(1));

            case MONTHLY:
                return indexMetadata.getIndexName() + monthlyDateTimeFormatter.print(currentDateTime.plusMonths(1));

            case YEARLY:
                return indexMetadata.getIndexName() + yearlyDateTimeFormatter.print(currentDateTime.plusYears(1));

            default:
                throw new UnsupportedAutoIndexException("Unsupported or invalid retention type (HOURLY, DAILY, MONTHLY, or YEARLY)");
        }
    }

    public static int getDateFromIndexName(IndexMetadata indexMetadata, String indexName) throws UnsupportedAutoIndexException {
        switch (indexMetadata.getRetentionType()) {
            case HOURLY:
                return Integer.parseInt(indexName.substring(indexName.length() - 10));

            case DAILY:
                return Integer.parseInt(indexName.substring(indexName.length() - 8));

            case MONTHLY:
                return Integer.parseInt(indexName.substring(indexName.length() - 6));

            case YEARLY:
                return Integer.parseInt(indexName.substring(indexName.length() - 4));

            default:
                throw new UnsupportedAutoIndexException("Unsupported or invalid retention type (HOURLY, DAILY, MONTHLY, or YEARLY)");
        }
    }

    /**
     * Convert the JSON String of parameters to IndexMetadata objects
     *
     * @param serializedIndexMetadata : JSON string with parameters
     * @return list of IndexMetadata objects
     * @throws IOException
     */
    public static List<IndexMetadata> parseIndexMetadata(String serializedIndexMetadata) throws IOException {
        ObjectMapper jsonMapper = new DefaultIndexMapper();
        TypeReference<List<IndexMetadata>> typeRef = new TypeReference<List<IndexMetadata>>() {};
        return jsonMapper.readValue(serializedIndexMetadata, typeRef);
    }
}