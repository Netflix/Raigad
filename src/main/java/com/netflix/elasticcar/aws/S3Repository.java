package com.netflix.elasticcar.aws;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.backup.RepositorySettingsDO;
import com.netflix.elasticcar.backup.RepositoryWrapperDO;
import com.netflix.elasticcar.backup.exception.CreateRepositoryException;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.SystemUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sloke on 7/1/14.
 */

/**
 * TODO: ADD following params to repo
 * The following settings are supported:

 bucket: The name of the bucket to be used for snapshots. (Mandatory)
 region: The region where bucket is located. Defaults to US Standard
 base_path: Specifies the path within bucket to repository data. Defaults to root directory.
 access_key: The access key to use for authentication. Defaults to value of cloud.aws.access_key.
 secret_key: The secret key to use for authentication. Defaults to value of cloud.aws.secret_key.
 chunk_size: Big files can be broken down into chunks during snapshotting if needed. The chunk size can be specified in bytes or by using size value notation, i.e. 1g, 10m, 5k. Defaults to 100m.
 compress: When set to true metadata files are stored in compressed format. This setting doesn't affect index files that are already compressed by default. Defaults to false.
 server_side_encryption: When set to true files are encrypted on server side using AES256 algorithm. Defaults to false.
 max_retries: Number of retries in case of S3 errors. Defaults to 3.

 */
@Singleton
public class S3Repository extends AbstractRepository
{
    private static final Logger logger = LoggerFactory.getLogger(S3Repository.class);
    private static final String S3_REPO_DATE_FORMAT = "yyyyMMdd";
    private static final DateTimeZone currentZone = DateTimeZone.UTC;

    @Inject
    private S3Repository(IConfiguration config)
    {
        super(config);
    }

    @Override
    public void initializeRepository(RepositoryType repositoryType)
    {
        this.bucket = config.getBackupLocation();
        this.clusterName = config.getAppName();
        this.region = config.getDC();
        this.type = repositoryType;
        this.basePath = config.getAppName().toLowerCase() + PATH_SEP + getRemoteRepositoryName();//basePathLocator.getSnapshotBackupBasePath();
    }

    /**
     * 0.0.0.0:9200/_snapshot/s3_repo
     * { "type": "s3",
     * 	 "settings": { "bucket": "us-east-1.netflix-cassandra-archive-test",
     * 	               "base_path": "es_abc/20140410",
     *                 "region": "us-east-1"
     *                }
     * }
     */
    @Override
    public String createOrGetRepository(RepositoryType repositoryType) throws Exception
    {
        String s3RepoName = null;
        try {
            initializeRepository(repositoryType);
            s3RepoName = getRemoteRepositoryName();
            //Check if Repository Exists
            if (!doesRepositoryExists(s3RepoName, repositoryType)) {
                //Creating New Repository now
                String URL = "http://127.0.0.1:" + config.getHttpPort() + "/_snapshot/" + s3RepoName;
                RepositorySettingsDO repositorySettingsDO = new RepositorySettingsDO(region, basePath, bucket);
                RepositoryWrapperDO repositoryWrapperDO = new RepositoryWrapperDO(type.toString().toLowerCase(), repositorySettingsDO);
                String jsonBody = mapper.writeValueAsString(repositoryWrapperDO);
                if (config.isDebugEnabled())
                    logger.info("Create Repository JSON : " + jsonBody);
                String response = SystemUtils.runHttpPostCommand(URL, jsonBody);
                if (response == null || response.isEmpty()) {
                    logger.error("Response from URL : <" + URL + "> is Null or Empty, hence stopping the current running thread");
                    throw new CreateRepositoryException("Response from URL : <" + URL + "> is Null or Empty, Creation of Repository failed !!");
                }
                logger.info("Response from URL : <" + URL + ">  = [" + response + "]. Successfully created a repository <" + s3RepoName + ">");
            }
        }
        catch (Exception e)
        {
            throw new CreateRepositoryException("Creation of Repository failed !!",e);
        }

        return s3RepoName;
    }

    @Override
    public String getRemoteRepositoryName() {
        DateTime dt = new DateTime();
        DateTime dtGmt = dt.withZone(currentZone);
        return SystemUtils.formatDate(dtGmt,S3_REPO_DATE_FORMAT);
    }

}
