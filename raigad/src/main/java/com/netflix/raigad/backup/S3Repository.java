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
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.utils.ESTransportClient;
import com.netflix.raigad.utils.SystemUtils;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * 	 "settings": { "bucket": "us-east-1.es-test",
     * 	               "base_path": "es_abc/20140410",
     *                 "region": "us-east-1"
     *                }
     * }
     */

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
        Client esTransportClient = ESTransportClient.instance(config).getTransportClient();
        //Creating New Repository now
        PutRepositoryResponse putRepositoryResponse = getPutRepositoryResponse(esTransportClient,s3RepoName);

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

    /**
     * Following method is isolated so that it helps in Unit Testing for Mocking
     * @param esTransportClient
     * @param s3RepoName
     * @return
     */
    public PutRepositoryResponse getPutRepositoryResponse(Client esTransportClient,String s3RepoName)
    {
        return esTransportClient.admin().cluster().preparePutRepository(s3RepoName)
                .setType(getRepositoryType().name()).setSettings(ImmutableSettings.settingsBuilder()
                                .put("base_path", repositorySettingsParams.getBase_path())
                                .put("region", repositorySettingsParams.getRegion())
                                .put("bucket", repositorySettingsParams.getBucket())
                ).get();
    }
}
