package com.netflix.elasticcar.backup.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class CreateRepositoryException extends Exception {

    private static final long serialVersionUID = 1L;

    public CreateRepositoryException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public CreateRepositoryException(String msg)
    {
        super(msg);
    }

    public CreateRepositoryException(Exception ex)
    {
        super(ex);
    }

    public CreateRepositoryException(Throwable th)
    {
        super(th);
    }
}
