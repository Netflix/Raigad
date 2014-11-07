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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.utils.ESTransportClient;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ImplementedBy(S3Repository.class)
public abstract class AbstractRepository
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRepository.class);

    public enum RepositoryType
    {
        s3,fs
    }

    protected final IConfiguration config;
    protected final AbstractRepositorySettingsParams repositorySettingsParams;

    @Inject
    protected AbstractRepository(IConfiguration config, @Named("s3")AbstractRepositorySettingsParams repositorySettingsParams)
    {
        this.config = config;
        this.repositorySettingsParams = repositorySettingsParams;
    }

    /**
     * Get Remote Repository Name
     */
    public abstract String getRemoteRepositoryName();

    public abstract String createOrGetSnapshotRepository() throws Exception;

    public abstract void createRestoreRepository(String s3RepoName, String basePathSuffix) throws Exception;

    public boolean  doesRepositoryExists(String repositoryName,RepositoryType repositoryType)
    {
        boolean doesRepoExists = false;
        logger.info("Checking if repository <"+repositoryName+"> exists for type <"+repositoryType.name()+">");

        try {

            Client esTransportClient = ESTransportClient.instance(config).getTransportClient();
            ClusterStateResponse clusterStateResponse = esTransportClient.admin().cluster().prepareState().clear().setMetaData(true).get();
            MetaData metaData = clusterStateResponse.getState().getMetaData();
            RepositoriesMetaData repositoriesMetaData = metaData.custom(RepositoriesMetaData.TYPE);
            if(repositoriesMetaData != null) {
                for (RepositoryMetaData repositoryMetaData : repositoriesMetaData.repositories()) {
                    if (repositoryMetaData.name().equalsIgnoreCase(repositoryName) && repositoryMetaData.type().equalsIgnoreCase(repositoryType.name())) {
                        doesRepoExists = true;
                        break;
                    }
                }
                if (config.isDebugEnabled())
                    for (RepositoryMetaData repositoryMetaData : repositoriesMetaData.repositories())
                        logger.debug("Repository <" + repositoryMetaData.name() + ">");

            }
            if (doesRepoExists)
                logger.info("Repository <" + repositoryName + "> already exists");
            else
                logger.info("Repository <" + repositoryName + "> does NOT exist");

        }
        catch(Exception e)
        {
            logger.warn("Exception thrown while listing Snapshot Repositories", e);
        }

        return doesRepoExists;
    }

}
