package com.netflix.elasticcar.backup;

import com.google.inject.Inject;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.objectmapper.DefaultRepositoryMapper;
import com.netflix.elasticcar.utils.ESTransportClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by sloke on 7/1/14.
 */
public abstract class AbstractRepository implements IRepository
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRepository.class);
    public static final char PATH_SEP = File.separatorChar;
    protected final ObjectMapper mapper = new DefaultRepositoryMapper();
    protected RepositoryType type;
    protected String bucket;
    protected String region;
    protected String basePath;
    protected String clusterName;

    protected final IConfiguration config;

    @Inject
    protected AbstractRepository(IConfiguration config) {
        this.config = config;
    }

    /**
     * Get remote repository path
     */
    public abstract String getRemoteRepositoryName();

    public void initializeRepository(RepositoryType repositoryType)
    {
        this.bucket = config.getBackupLocation();
        this.clusterName = config.getAppName();
        this.region = config.getDC();
        this.type = repositoryType;
        this.basePath = config.getAppName().toLowerCase();//basePathLocator.getSnapshotBackupBasePath();
    }

    public boolean  doesRepositoryExists(String repositoryName,RepositoryType repositoryType) //throws DuplicateRepositoryNameException
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
                if(repositoryMetaData.name().equalsIgnoreCase(repositoryName) && repositoryMetaData.type().equalsIgnoreCase(IRepository.RepositoryType.s3.toString()))
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


    public String getClusterName() {
        return clusterName;
    }

    public RepositoryType getType() {
        return type;
    }

    public String getBucket() {
        return bucket;
    }

    public String getRegion() {
        return region;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }


}
