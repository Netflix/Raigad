package com.netflix.elasticcar.backup.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class NoRepositoryException extends Exception {

    private static final long serialVersionUID = 1L;

    public NoRepositoryException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public NoRepositoryException(String msg)
    {
        super(msg);
    }

    public NoRepositoryException(Exception ex)
    {
        super(ex);
    }

    public NoRepositoryException(Throwable th)
    {
        super(th);
    }
}
