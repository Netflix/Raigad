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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.backup.exception.CreateRepositoryException;
import com.netflix.raigad.backup.exception.RestoreBackupException;
import com.netflix.raigad.configuration.IConfiguration;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

@Singleton
public class S3RepositorySettingsParams extends AbstractRepositorySettingsParams
{
    private static final Logger logger = LoggerFactory.getLogger(S3RepositorySettingsParams.class);
    private final char PATH_SEP = File.separatorChar;
    private final String S3_REPO_DATE_FORMAT = "yyyyMMdd";

    @Inject
    public S3RepositorySettingsParams(IConfiguration config) {
        super(config);
    }

    @Override
    public void setBackupParams() throws CreateRepositoryException {
        this.bucket = config.getBackupLocation();
        if(StringUtils.isEmpty(this.bucket))
            throw new CreateRepositoryException("Backup Location is not set in configuration.");
        this.region = config.getDC();
        this.base_path = getSnapshotBackupBasePath();
        logger.info("Bucket : <"+bucket+"> Region : <"+region+"> Base_path : <"+base_path+">");
    }

    @Override
    public void setRestoreParams(String basePathSuffix) throws RestoreBackupException {
        if(StringUtils.isNotBlank(config.getRestoreLocation()))
            this.bucket = config.getRestoreLocation();
        else {
            logger.info("config.getRestoreLocation() is Blank, hence setting bucket = config.getBackupLocation()");
            this.bucket = config.getBackupLocation();
        }

        if(StringUtils.isNotBlank(config.getRestoreSourceRepositoryRegion()))
            this.region = config.getRestoreSourceRepositoryRegion();
        else {
            logger.info("config.getRestoreSourceRepositoryRegion() is Blank, hence setting region = config.getDC()");
            this.region = config.getDC();
        }
        this.base_path = getRestoreBackupBasePath(basePathSuffix);
        logger.info("Bucket : <"+bucket+"> Region : <"+region+"> Base_path : <"+base_path+">");
    }

    //"base_path": "es_{current_cluster_name}/20140410"
    public String getSnapshotBackupBasePath()
    {
        StringBuilder basePath = new StringBuilder();
        basePath.append(config.getAppName());
        basePath.append(PATH_SEP);
        String repoSuffix = getS3RepositoryName();
        basePath.append(repoSuffix);
        logger.info("S3 Repository Snapshot Base Path : <"+basePath.toString()+">");
        return basePath.toString();
    }

    /*
        base_path = basePathPrefix + basePathSuffix
        Here you can provide custom base_path *Prefix* instead of using default source_cluster_name
     */
    //"base_path": "es_{source_cluster_name}/20140410"
    public String getRestoreBackupBasePath(String basePathSuffix) throws RestoreBackupException {
        StringBuilder basePath = new StringBuilder();
        if(StringUtils.isNotBlank(config.getRestoreSourceClusterName()))
            basePath.append(config.getRestoreSourceClusterName());
        else
            throw new RestoreBackupException("No Source Cluster for Restore yet chosen.");
        basePath.append(PATH_SEP);
        basePath.append(basePathSuffix);
        logger.info("S3 Repository Restore Base Path : <"+basePath.toString()+">");
        return basePath.toString();
    }

    public String getS3RepositoryName()
    {
        DateTime dt = new DateTime();
        DateTime dtGmt = dt.withZone(DateTimeZone.UTC);
        return formatDate(dtGmt,S3_REPO_DATE_FORMAT);
    }

    public String formatDate(DateTime dateTime, String dateFormat)
    {
        DateTimeFormatter fmt = DateTimeFormat.forPattern(dateFormat);
        return dateTime.toString(fmt);
    }
}
