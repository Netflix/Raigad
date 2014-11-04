package com.netflix.elasticcar.backup.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class SnapshotBackupException extends Exception {

    private static final long serialVersionUID = 1L;

    public SnapshotBackupException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public SnapshotBackupException(String msg)
    {
        super(msg);
    }

    public SnapshotBackupException(Exception ex)
    {
        super(ex);
    }

    public SnapshotBackupException(Throwable th)
    {
        super(th);
    }
}
