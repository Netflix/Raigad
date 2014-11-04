package com.netflix.elasticcar.indexmanagement.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class UnsupportedAutoIndexException extends Exception {

    private static final long serialVersionUID = 1L;

    public UnsupportedAutoIndexException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public UnsupportedAutoIndexException(String msg)
    {
        super(msg);
    }

    public UnsupportedAutoIndexException(Exception ex)
    {
        super(ex);
    }

    public UnsupportedAutoIndexException(Throwable th)
    {
        super(th);
    }
}
