package com.netflix.elasticcar.backup.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class RestoreBackupException extends Exception {

    private static final long serialVersionUID = 1L;

    public RestoreBackupException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public RestoreBackupException(String msg)
    {
        super(msg);
    }

    public RestoreBackupException(Exception ex)
    {
        super(ex);
    }

    public RestoreBackupException(Throwable th)
    {
        super(th);
    }
}
