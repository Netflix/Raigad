package com.netflix.elasticcar.backup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.backup.exception.RestoreBackupException;
import com.netflix.elasticcar.configuration.IConfiguration;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by sloke on 7/16/14.
 */
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
    public void setBackupParams()
    {
        this.bucket = config.getBackupLocation();
        this.region = config.getDC();
        this.base_path = getSnapshotBackupBasePath();
        logger.info("Bucket : <"+bucket+"> Region : <"+region+"> Base_path : <"+base_path+">");
    }

    @Override
    public void setRestoreParams(String repoName) throws RestoreBackupException {
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
        this.base_path = getRestoreBackupBasePath(repoName);
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

    //"base_path": "es_{source_cluster_name}/20140410"
    public String getRestoreBackupBasePath(String repoName) throws RestoreBackupException {
        StringBuilder basePath = new StringBuilder();
        if(StringUtils.isNotBlank(config.getRestoreSourceClusterName()))
            basePath.append(config.getRestoreSourceClusterName());
        else
            throw new RestoreBackupException("No Source Cluster for Restore yet chosen.");
        basePath.append(PATH_SEP);
        basePath.append(repoName);
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
