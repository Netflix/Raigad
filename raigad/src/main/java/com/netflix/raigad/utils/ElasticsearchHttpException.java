package com.netflix.raigad.utils;

import java.io.IOException;

public class ElasticsearchHttpException extends IOException
{
    private static final long serialVersionUID = 444L;

    public ElasticsearchHttpException(String message)
    {
        super(message);
    }

    public ElasticsearchHttpException(String message, Exception e)
    {
        super(message, e);
    }
}
