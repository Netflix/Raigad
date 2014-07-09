package com.netflix.elasticcar.aws;

import com.netflix.elasticcar.backup.exception.DuplicateRepositoryNameException;

/**
 * Created by sloke on 7/2/14.
 */
public interface IRepository
{
    public enum RepositoryType
    {
        s3, fs, azure
    }

    public boolean doesRepositoryExists(String repositoryName,RepositoryType repositoryType) throws DuplicateRepositoryNameException;

    public void initializeRepository(RepositoryType repositoryType);

    /**
     *
     * @param repositoryType
     * @return  repositoryName
     * @throws Exception
     */
    public String createOrGetRepository(RepositoryType repositoryType) throws Exception;

    public String getRemoteRepositoryName();
}
