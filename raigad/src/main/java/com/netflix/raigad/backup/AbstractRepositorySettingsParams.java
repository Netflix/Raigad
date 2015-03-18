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
package com.netflix.raigad.backup;

import com.google.inject.ImplementedBy;
import com.netflix.raigad.backup.exception.CreateRepositoryException;
import com.netflix.raigad.backup.exception.RestoreBackupException;
import com.netflix.raigad.configuration.IConfiguration;

@ImplementedBy(S3RepositorySettingsParams.class)
public abstract class AbstractRepositorySettingsParams {
    /**
     * 0.0.0.0:9200/_snapshot/20140410
     * { "type": "s3",
     * "settings": { "bucket": "us-east-1.netflix-cassandra-archive-test",
     * "base_path": "es_abc/20140410",
     * "region": "us-east-1"
     * }
     * }
     */
    protected String bucket;
    protected String base_path;
    protected String region;

    protected final IConfiguration config;

    public AbstractRepositorySettingsParams(IConfiguration config) {
        this.config = config;
    }

    public abstract void setBackupParams() throws CreateRepositoryException;

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
