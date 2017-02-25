/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.indexmanagement.exception.UnsupportedAutoIndexException;
import com.netflix.raigad.indexmanagement.indexfilters.DailyIndexNameFilter;
import com.netflix.raigad.indexmanagement.indexfilters.MonthlyIndexNameFilter;
import com.netflix.raigad.indexmanagement.indexfilters.YearlyIndexNameFilter;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;


public class IndexMetadata {

    public enum RETENTION_TYPE {
        DAILY, MONTHLY, YEARLY
    }

    private final String indexName;
    private final RETENTION_TYPE retentionType;
    private final Integer retentionPeriod;
    private final IIndexNameFilter indexNameFilter;
    private final boolean preCreate;

    @JsonCreator
    public IndexMetadata(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("retentionType") String retentionType,
            @JsonProperty("retentionPeriod") Integer retentionPeriod,
            @JsonProperty("preCreate") Boolean preCreate) throws UnsupportedAutoIndexException {

        this.indexName = indexName;

        if (retentionType == null) {
            retentionType = "DAILY";
        }

        this.retentionType = RETENTION_TYPE.valueOf(retentionType.toUpperCase());

        switch(this.retentionType) {
            case DAILY:
                this.indexNameFilter = new DailyIndexNameFilter();
                break;

            case MONTHLY:
                this.indexNameFilter = new MonthlyIndexNameFilter();
                break;

            case YEARLY:
                this.indexNameFilter = new YearlyIndexNameFilter();
                break;

            default:
                this.indexNameFilter = null;
                throw new UnsupportedAutoIndexException("Unsupported or invalid retention type (DAILY or MONTHLY or YEARLY), please check your configuration");
        }

        this.retentionPeriod = retentionPeriod;

        if (preCreate == null) {
            this.preCreate = false;
        } else {
            this.preCreate = preCreate;
        }
    }

    @Override
    public String toString() {
        return String.format("{\"indexName\":\"%s\",\"retentionType\":\"%s\",\"retentionPeriod\":%n,\"preCreate\":%b}",
                indexName, retentionType, retentionPeriod, preCreate);
    }

    public String getIndexName() {
        return indexName;
    }

    public RETENTION_TYPE getRetentionType() {
        return retentionType;
    }

    public Integer getRetentionPeriod() {
        return retentionPeriod;
    }

    public IIndexNameFilter getIndexNameFilter() {
        return indexNameFilter;
    }

    public boolean isPreCreate() {
        return preCreate;
    }

    public boolean isActionable() {
        return indexName != null && retentionPeriod != null;
    }
}
