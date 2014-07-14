package com.netflix.elasticcar.backup;

import com.google.inject.Inject;
import com.netflix.elasticcar.objectmapper.DefaultRepositoryMapper;
import com.netflix.elasticcar.backup.exception.DuplicateRepositoryNameException;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.SystemUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

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

    public boolean  doesRepositoryExists(String repositoryName,RepositoryType repositoryType) throws DuplicateRepositoryNameException
    {
        boolean doesRepoExists = false;

        try {
            String URL =  "http://127.0.0.1:" + config.getHttpPort() + "/_snapshot/";
            String response = SystemUtils.runHttpGetCommand(URL);
            if (config.isDebugEnabled()) {
                logger.debug("Calling URL API: {} returns: {}", URL, response);
            }
            //Split the response on Spaces to get IP
            if (response == null || response.isEmpty()) {
                logger.error("Response from URL : <" + URL + "> is Null or Empty, hence repository does not exist");
                return false;
            }

            TypeReference<Map<String, RepositoryWrapperDO>> typeReference = new TypeReference<Map<String, RepositoryWrapperDO>>() {};

            Map<String, RepositoryWrapperDO> existingRepositoryMap = mapper.readValue(response, typeReference);

            if(existingRepositoryMap != null) {
                for (String existingRepoName : existingRepositoryMap.keySet()) {
                    if (existingRepoName.toLowerCase().equalsIgnoreCase(repositoryName)) {
                        if(!existingRepositoryMap.get(existingRepoName).getType().equalsIgnoreCase(repositoryType.toString()))
                        {
                            throw new DuplicateRepositoryNameException("Repository with name : <"+repositoryName+"> already exists but with Type : <"+existingRepositoryMap.get(existingRepoName).getType()+">");
                        }
                        if (config.isDebugEnabled())
                            logger.debug("Repository = <" + repositoryName + "> already exists.");

                        doesRepoExists = true;
                        break;
                    }
                }
            }
        }
        catch(DuplicateRepositoryNameException drne)
        {
            throw drne;
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
