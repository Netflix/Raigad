package com.netflix.raigad.utils;

import java.io.IOException;

public class ESTransportClientConnectionException extends IOException
{

    private static final long serialVersionUID = 444L;

    public ESTransportClientConnectionException(String message)
    {
        super(message);
    }

    public ESTransportClientConnectionException(String message, Exception e)
    {
        super(message, e);
    }

}
