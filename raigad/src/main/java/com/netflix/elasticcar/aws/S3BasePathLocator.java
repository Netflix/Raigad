package com.netflix.elasticcar.aws;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.configuration.IConfiguration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by sloke on 7/2/14.
 */
@Deprecated
@Singleton
public class S3BasePathLocator implements IBasePathLocator
{
    private static final Logger logger = LoggerFactory.getLogger(S3BasePathLocator.class);
    private final char PATH_SEP = File.separatorChar;
    private final String S3_REPO_DATE_FORMAT = "yyyyMMdd";
    private final IConfiguration config;

    @Inject
    public S3BasePathLocator(IConfiguration config)
    {
        this.config = config;
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

    //"base_path": "es_{current_cluster_name}/20140410"
    @Override
    public String getSnapshotBackupBasePath()
    {
        StringBuilder basePath = new StringBuilder();
        basePath.append(config.getAppName());
        basePath.append(PATH_SEP);
        String repoSuffix = getS3RepositoryName();
        basePath.append(repoSuffix);
        if(config.isDebugEnabled())
            logger.debug("S3 Repository Snapshot Base Path : <"+basePath.toString()+">");
        return basePath.toString();
    }

    //"base_path": "es_{source_cluster_name}/20140410"
    @Override
    public String getRestoreBackupBasePath() {
        StringBuilder basePath = new StringBuilder();

        basePath.append(config.getRestoreSourceClusterName());
        basePath.append(PATH_SEP);
        String repoSuffix = config.getRestoreRepositoryName();
        basePath.append(repoSuffix);
        if(config.isDebugEnabled())
            logger.debug("S3 Repository Restore Base Path : <"+basePath.toString()+">");
        return basePath.toString();
    }
}
