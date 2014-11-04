package com.netflix.elasticcar.utils;

import java.io.IOException;

public class ESHttpException extends IOException
{

    private static final long serialVersionUID = 444L;

    public ESHttpException(String message)
    {
        super(message);
    }

    public ESHttpException(String message, Exception e)
    {
        super(message, e);
    }

}
