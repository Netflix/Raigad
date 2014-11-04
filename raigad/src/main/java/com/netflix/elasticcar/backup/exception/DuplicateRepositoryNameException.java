package com.netflix.elasticcar.backup.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class DuplicateRepositoryNameException extends Exception {

    private static final long serialVersionUID = 1L;

    public DuplicateRepositoryNameException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public DuplicateRepositoryNameException(String msg)
    {
        super(msg);
    }

    public DuplicateRepositoryNameException(Exception ex)
    {
        super(ex);
    }

    public DuplicateRepositoryNameException(Throwable th)
    {
        super(th);
    }
}
