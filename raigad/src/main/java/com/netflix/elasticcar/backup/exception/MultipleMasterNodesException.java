package com.netflix.elasticcar.backup.exception;

/**
 * Created by sloke on 6/26/14.
 */
public class MultipleMasterNodesException extends Exception {

    private static final long serialVersionUID = 1L;

    public MultipleMasterNodesException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public MultipleMasterNodesException(String msg)
    {
        super(msg);
    }

    public MultipleMasterNodesException(Exception ex)
    {
        super(ex);
    }

    public MultipleMasterNodesException(Throwable th)
    {
        super(th);
    }
}
