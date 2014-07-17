package com.netflix.elasticcar.backup;

import com.google.inject.ImplementedBy;
import com.google.inject.Inject;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.ESTransportClient;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sloke on 7/1/14.
 */
@ImplementedBy(S3Repository.class)
public abstract class AbstractRepository
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRepository.class);

    public enum RepositoryType
    {
        s3
    }

    protected final IConfiguration config;
    protected final AbstractRepositorySettingsParams repositorySettingsParams;

    @Inject
    protected AbstractRepository(IConfiguration config, AbstractRepositorySettingsParams repositorySettingsParams)
    {
        this.config = config;
        this.repositorySettingsParams = repositorySettingsParams;
    }

    /**
     * Get Remote Repository Name
     */
    public abstract String getRemoteRepositoryName();

//    public abstract String createOrGetRepository(ActionType actionType) throws Exception;

    public abstract String createOrGetSnapshotRepository() throws Exception;

    public abstract void createRestoreRepository(String s3RepoName) throws Exception;

    public boolean  doesRepositoryExists(String repositoryName,RepositoryType repositoryType)
    {
        boolean doesRepoExists = false;
        logger.info("Checking if repository <"+repositoryName+"> exists for type <"+repositoryType.name()+">");

        try {

            TransportClient esTransportClient = ESTransportClient.instance(config).getTransportClient();

            ClusterStateResponse clusterStateResponse = esTransportClient.admin().cluster().prepareState().clear().setMetaData(true).get();
            MetaData metaData = clusterStateResponse.getState().getMetaData();
            RepositoriesMetaData repositoriesMetaData = metaData.custom(RepositoriesMetaData.TYPE);

            for (RepositoryMetaData repositoryMetaData : repositoriesMetaData.repositories())
            {
                if(repositoryMetaData.name().equalsIgnoreCase(repositoryName) && repositoryMetaData.type().equalsIgnoreCase(repositoryType.name()))
                {
                    doesRepoExists = true;
                    break;
                }
            }

            if(config.isDebugEnabled())
                for (RepositoryMetaData repositoryMetaData : repositoriesMetaData.repositories())
                    logger.debug("Repository <" + repositoryMetaData.name() + ">");

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
