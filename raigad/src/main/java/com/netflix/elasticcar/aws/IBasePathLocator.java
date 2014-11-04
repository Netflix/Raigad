package com.netflix.elasticcar.aws;

/**
 * Created by sloke on 7/2/14.
 */
@Deprecated
public interface IBasePathLocator
{
    public String getSnapshotBackupBasePath();

    public String  getRestoreBackupBasePath();
}
