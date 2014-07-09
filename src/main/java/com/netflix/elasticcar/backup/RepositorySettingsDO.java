package com.netflix.elasticcar.backup;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by sloke on 7/7/14.
 */
/*
    {
        "20140331": {
            "type": "s3",
            "settings": {
                "region": "us-east-1",
                "base_path": "es_chronos/20140331",
                "bucket": "elasticsearch-backup-prod"
            }
        },
        "20140410": {
            "type": "s3",
            "settings": {
                "region": "us-east-1",
                "base_path": "es_chronos/20140410",
                "bucket": "elasticsearch-backup-prod"
            }
        }
    }
 */
public class RepositorySettingsDO
{
    private final String region;
    private final String base_path;
    private final String bucket;

    @JsonCreator
    public RepositorySettingsDO(@JsonProperty("region") final String region,
                         @JsonProperty("base_path") final String base_path,
                         @JsonProperty("bucket") final String bucket)
    {
        this.region = region;
        this.base_path = base_path;
        this.bucket = bucket;
    }

    public String getRegion() {
        return region;
    }

    public String getBase_path() {
        return base_path;
    }

    public String getBucket() {
        return bucket;
    }
}
