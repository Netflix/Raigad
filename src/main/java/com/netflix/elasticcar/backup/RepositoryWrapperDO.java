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
public class RepositoryWrapperDO
{
    private final String type;
    private final RepositorySettingsDO settings;

    @JsonCreator
    public RepositoryWrapperDO(@JsonProperty("type") final String type,
                        @JsonProperty("settings") final RepositorySettingsDO settings)
    {
        this.type = type;
        this.settings = settings;
    }

    public String getType() {
        return type;
    }

    public RepositorySettingsDO getSettings() {
        return settings;
    }

}
