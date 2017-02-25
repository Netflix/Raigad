package com.netflix.raigad.utils;

import java.io.IOException;

public class ElasticsearchTransportClientConnectionException extends IOException
{
    private static final long serialVersionUID = 444L;

    public ElasticsearchTransportClientConnectionException(String message)
    {
        super(message);
    }

    public ElasticsearchTransportClientConnectionException(String message, Exception e)
    {
        super(message, e);
    }
}
