package com.netflix.elasticcar.indexmanagement;

import com.netflix.elasticcar.indexmanagement.exception.UnsupportedAutoIndexException;
import com.netflix.elasticcar.indexmanagement.indexfilters.DailyIndexNameFilter;
import com.netflix.elasticcar.indexmanagement.indexfilters.MonthlyIndexNameFilter;
import com.netflix.elasticcar.indexmanagement.indexfilters.YearlyIndexNameFilter;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Courtesy: Jae Bae
 */
public class IndexMetadata {

    public enum RETENTION_TYPE {
        DAILY, MONTHLY, YEARLY
    }

    private final String indexName;
    private final RETENTION_TYPE retentionType;
    private final int retentionPeriod;
    private final IIndexNameFilter indexNameFilter;
    private final boolean preCreate;

    @JsonCreator
    public IndexMetadata(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("retentionType") String retentionType,
            @JsonProperty("retentionPeriod") int retentionPeriod,
            @JsonProperty("preCreate") boolean preCreate) throws UnsupportedAutoIndexException {

        this.indexName = indexName;

        if(retentionType == null)
           retentionType = "DAILY";

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
                throw new UnsupportedAutoIndexException("Given index is not (DAILY or MONTHLY or YEARLY), please check your configuration.");
        }
        this.retentionPeriod = retentionPeriod;
        this.preCreate = preCreate;
    }

    @Override
    public String toString() {
        return "IndexMetadata{" +
                "indexName='" + indexName + '\'' +
                ", retentionType=" + retentionType +
                ", retentionPeriod=" + retentionPeriod +
                '}';
    }

    public String getIndexName() {
        return indexName;
    }

    public RETENTION_TYPE getRetentionType() {
        return retentionType;
    }

    public int getRetentionPeriod() {
        return retentionPeriod;
    }

    public IIndexNameFilter getIndexNameFilter() {
        return indexNameFilter;
    }

    public boolean isPreCreate() {
        return preCreate;
    }

}
