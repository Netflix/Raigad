package com.netflix.elasticcar.backup;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by sloke on 7/7/14.
 */
	/*
	 * ec2-50-19-28-170.compute-1.amazonaws.com:7104/_snapshot/20140320/snapshot_1?wait_for_completion=true
	 *
	 * {
     *  "indices": "index_1,index_2",  //  "indices": "_all",
     *  "ignore_unavailable": "true",
     *  "include_global_state": false
     * }
	 *
	 */
public class SnapshotSettingsDO
{
    private final String indices;
    private final String ignore_unavailable;
    private final boolean include_global_state;

    @JsonCreator
    public SnapshotSettingsDO(@JsonProperty("indices") final String indices,
                              @JsonProperty("ignore_unavailable") final String ignore_unavailable,
                              @JsonProperty("include_global_state") final boolean include_global_state)
    {
        this.indices = indices;
        this.ignore_unavailable = ignore_unavailable;
        this.include_global_state = include_global_state;
    }

    public String getIndices() {
        return indices;
    }

    public String getIgnore_unavailable() {
        return ignore_unavailable;
    }

    public boolean isInclude_global_state() {
        return include_global_state;
    }
}

