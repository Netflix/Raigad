package com.netflix.elasticcar.backup.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class CreateSnapshotException extends Exception {

    private static final long serialVersionUID = 1L;

    public CreateSnapshotException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public CreateSnapshotException(String msg)
    {
        super(msg);
    }

    public CreateSnapshotException(Exception ex)
    {
        super(ex);
    }

    public CreateSnapshotException(Throwable th)
    {
        super(th);
    }
}
