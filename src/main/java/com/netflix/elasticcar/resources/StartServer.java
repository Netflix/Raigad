package com.netflix.elasticcar.resources;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.server.base.BaseServer;

/**
 * Startup servlet which will be used to initialize this netflix-style web application.
 *
 */
public class StartServer extends BaseServer
{
    private static final Logger logger = LoggerFactory.getLogger(StartServer.class);
    private static final String CONFIG_NAME = "ElasticCar";
    private static final String APP_VERSION = "1.0";

    public StartServer()
    {
        this(CONFIG_NAME, PreStartServer.APP_NAME, APP_VERSION);
    }

    protected StartServer(String configName, String appName, String appVersion)
    {
        super(configName, appName, appVersion);
    }


    protected void initialize(Properties props) throws Exception
    {
        //nop
    }
}
