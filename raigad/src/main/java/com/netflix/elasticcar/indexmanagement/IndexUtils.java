/**
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.elasticcar.indexmanagement;

import com.netflix.elasticcar.indexmanagement.exception.UnsupportedAutoIndexException;
import org.elasticsearch.client.transport.TransportClient;
import org.joda.time.DateTime;

public class IndexUtils {

    public static void getIndicesStatus(TransportClient client) {

    }

    public static int getPastRetentionCutoffDate(IndexMetadata indexMetadata) throws UnsupportedAutoIndexException {

        DateTime dt = new DateTime();
        int currentDate;

        switch (indexMetadata.getRetentionType()) {
            case DAILY:
                dt = dt.minusDays(indexMetadata.getRetentionPeriod());
                currentDate = Integer.parseInt(String.format("%d%02d%02d", dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth()));
                break;
            case MONTHLY:
                dt = dt.minusMonths(indexMetadata.getRetentionPeriod());
                currentDate = Integer.parseInt(String.format("%d%02d", dt.getYear(), dt.getMonthOfYear()));
                break;
            case YEARLY:
                dt = dt.minusYears(indexMetadata.getRetentionPeriod());
                currentDate = Integer.parseInt(String.format("%d", dt.getYear()));
                break;
            default:
                throw new UnsupportedAutoIndexException("Given index is not (DAILY or MONTHLY or YEARLY), please check your configuration.");

        }
        return currentDate;
    }

    public static int getDateFromIndexName(IndexMetadata indexMetadata,String indexName) throws UnsupportedAutoIndexException{

        int indexDate;

        switch (indexMetadata.getRetentionType()) {
            case DAILY:
                indexDate = Integer.parseInt(indexName.substring(indexName.length() - 8));
                break;
            case MONTHLY:
                indexDate = Integer.parseInt(indexName.substring(indexName.length() - 6));
                break;
            case YEARLY:
                indexDate = Integer.parseInt(indexName.substring(indexName.length() - 4));
                break;
            default:
                throw new UnsupportedAutoIndexException("Given index is not (DAILY or MONTHLY or YEARLY), please check your configuration.");

        }
        return indexDate;
    }

    public static int getFutureRetentionDate(IndexMetadata indexMetadata) throws UnsupportedAutoIndexException{

        DateTime dt = new DateTime();
        int currentDate;

        switch (indexMetadata.getRetentionType()) {
            case DAILY:
                dt = dt.plusDays(indexMetadata.getRetentionPeriod());
                currentDate = Integer.parseInt(String.format("%d%02d%02d", dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth()));
                break;
            case MONTHLY:
                dt = dt.plusMonths(indexMetadata.getRetentionPeriod());
                currentDate = Integer.parseInt(String.format("%d%02d", dt.getYear(), dt.getMonthOfYear()));
                break;
            case YEARLY:
                dt = dt.plusYears(indexMetadata.getRetentionPeriod());
                currentDate = Integer.parseInt(String.format("%d", dt.getYear()));
                break;
            default:
                throw new UnsupportedAutoIndexException("Given index is not (DAILY or MONTHLY or YEARLY), please check your configuration.");

        }
        return currentDate;
    }
}
