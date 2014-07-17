package com.netflix.elasticcar.backup;

import com.google.inject.ImplementedBy;
import com.netflix.elasticcar.backup.exception.RestoreBackupException;
import com.netflix.elasticcar.configuration.IConfiguration;

/**
 * Created by sloke on 7/16/14.
 */
@ImplementedBy(S3RepositorySettingsParams.class)
public abstract class AbstractRepositorySettingsParams
{
    /**
     * 0.0.0.0:9200/_snapshot/20140410
     * { "type": "s3",
     * 	 "settings": { "bucket": "us-east-1.netflix-cassandra-archive-test",
     * 	               "base_path": "es_abc/20140410",
     *                 "region": "us-east-1"
     *                }
     * }
     */
    protected String bucket;
    protected String base_path;
    protected String region;

    protected final IConfiguration config;

    public AbstractRepositorySettingsParams(IConfiguration config)
    {
       this.config = config;
    }

    public abstract void setBackupParams();

    public abstract void setRestoreParams(String basePathSuffix) throws RestoreBackupException;

    public String getBucket() {
        return bucket;
    }

    public String getBase_path() {
        return base_path;
    }

    public String getRegion() {
        return region;
    }

}
