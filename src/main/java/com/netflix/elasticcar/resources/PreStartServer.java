package com.netflix.elasticcar.resources;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PreStartServer implements ServletContextListener
{
    private static final Logger logger = LoggerFactory.getLogger(PreStartServer.class);
    static final String bootPropFileName = "/etc/Elasticsearch.properties";
    static final String APP_NAME = deriveAppName();

    static String deriveAppName()
    {
        final String name = System.getenv("NETFLIX_APP");
        System.setProperty("netflix.appinfo.name", name);
        return name;
    }

    public void contextInitialized(ServletContextEvent servletContextEvent)
    {
        /*
            OK, this is messy as we need to tell platform not to use platform service for
            properties if this is a cass_turtle instance.
         */
        if (new File(bootPropFileName).exists())
        {
            logger.info("found /etc/Elasticsearch.properties file; reading those props for override");
            try
            {
                Properties p = new Properties();
                p.load(new FileReader(bootPropFileName));
                for(String s : p.stringPropertyNames())
                {
                    System.setProperty(s, p.get(s).toString().trim());
                }
            }
            catch(Exception e)
            {
                throw new RuntimeException("failed to load " + bootPropFileName, e);
            }
        }
        else
        {
            logger.info("did not find /etc/Elasticsearch.properties file; normal reading of properties will occur.");
        }
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent)
    {

    }
}
