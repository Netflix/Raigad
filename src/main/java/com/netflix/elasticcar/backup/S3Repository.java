package com.netflix.elasticcar.backup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.backup.exception.CreateRepositoryException;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.ESTransportClient;
import com.netflix.elasticcar.utils.SystemUtils;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
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
    private RepositoryType type;
    private AbstractRepositorySettingsParams repositorySettingsParams;

    @Inject
    private S3Repository(IConfiguration config,AbstractRepositorySettingsParams repositorySettingsParams)
    {
        super(config,repositorySettingsParams);
        this.type = RepositoryType.s3;
        this.repositorySettingsParams = repositorySettingsParams;
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
//    @Override
//    public String createOrGetRepository(ActionType actionType) throws Exception
//    {
//        logger.info("Trying to create or get repository of type <"+getRepositoryType().name()+">");
//
//        String s3RepoName = null;
//           if(actionType == ActionType.SNAPSHOT)
//            {
//                s3RepoName = createOrGetSnapshotRepository();
//            } else if (actionType == ActionType.RESTORE)
//            {
//                s3RepoName = createOrGetRestoreRepository();
//            }
//
//        return s3RepoName;
//    }

    @Override
    public String createOrGetSnapshotRepository() throws Exception
    {
        String s3RepoName = null;
        try {

            s3RepoName = getRemoteRepositoryName();
            logger.info("Snapshot Repository Name : <"+s3RepoName+">");

            //Set Snapshot Backup related parameters
            repositorySettingsParams.setBackupParams();

            //Check if Repository Exists
            if (!doesRepositoryExists(s3RepoName, getRepositoryType())) {
                createNewRepository(s3RepoName);
            }
        }
        catch (Exception e)
        {
            throw new CreateRepositoryException("Creation of Snapshot Repository failed !!",e);
        }

        return s3RepoName;
    }

    @Override
    public void createRestoreRepository(String s3RepoName, String basePathSuffix) throws Exception
    {
        try {
            //Set Restore related parameters
            repositorySettingsParams.setRestoreParams(basePathSuffix);

            //Check if Repository Exists
            createNewRepository(s3RepoName);
        }
        catch (Exception e)
        {
            throw new CreateRepositoryException("Creation of Restore Repository failed !!",e);
        }
    }

    public void createNewRepository(String s3RepoName) throws Exception
    {
        TransportClient esTransportClient = ESTransportClient.instance(config).getTransportClient();

        //Creating New Repository now
        PutRepositoryResponse putRepositoryResponse = esTransportClient.admin().cluster().preparePutRepository(s3RepoName)
                .setType(getRepositoryType().name()).setSettings(ImmutableSettings.settingsBuilder()
                                .put("base_path", repositorySettingsParams.getBase_path())
                                .put("region", repositorySettingsParams.getRegion())
                                .put("bucket", repositorySettingsParams.getBucket())
                ).get();

        if(putRepositoryResponse.isAcknowledged())
        {
            logger.info("Successfully created a repository : <" + s3RepoName + "> " + getRepoParamPrint());
        }
        else {
            throw new CreateRepositoryException("Creation of repository failed : <" + s3RepoName + "> " +
                    getRepoParamPrint());
        }
    }

    @Override
    public String getRemoteRepositoryName() {
        DateTime dt = new DateTime();
        DateTime dtGmt = dt.withZone(currentZone);
        return SystemUtils.formatDate(dtGmt,S3_REPO_DATE_FORMAT);
    }

    public RepositoryType getRepositoryType()
    {
        return type;
    }

    public String getRepoParamPrint()
    {
        return  "bucket : <"+repositorySettingsParams.getBucket()+"> " +
                "base_path : <"+repositorySettingsParams.getBase_path()+"> " +
                "region : <"+repositorySettingsParams.getRegion()+">";
    }

}
