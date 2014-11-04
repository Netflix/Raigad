package com.netflix.elasticcar.backup.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class NoMasterNodeException extends Exception {

    private static final long serialVersionUID = 1L;

    public NoMasterNodeException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public NoMasterNodeException(String msg)
    {
        super(msg);
    }

    public NoMasterNodeException(Exception ex)
    {
        super(ex);
    }

    public NoMasterNodeException(Throwable th)
    {
        super(th);
    }
}
