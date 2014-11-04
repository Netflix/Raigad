package com.netflix.elasticcar.backup;

/**
 * Created by sloke on 7/2/14.
 */
@Deprecated
public interface IRepository
{
    public enum RepositoryType
    {
        s3, fs, azure
    }

    public boolean doesRepositoryExists(String repositoryName,RepositoryType repositoryType) ;

    /**
     *
     * @param repositoryType
     * @return  repositoryName
     * @throws Exception
     */
    public String createOrGetRepository(RepositoryType repositoryType) throws Exception;

    public String getRemoteRepositoryName();
}
