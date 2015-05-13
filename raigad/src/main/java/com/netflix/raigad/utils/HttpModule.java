package com.netflix.raigad.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;

/**
 * Created by sloke on 7/10/14.
 */
@Singleton
public class HttpModule
{
    private static final String HTTP_TAG = "http://";
    private static final String LOCAL_HOST = "127.0.0.1";
    private static final String URL_PORT_SEPARATOR = ":";
    public static final String URL_PATH_SEPARATOR = "/";
    private static final String MASTER_NODE_SUFFIX = "/_cat/master";
    private static final String SNAPSHOT_BKP_KEYWORD = "/_snapshot/";
    private static final String SNAPSHOT_BKP_WAIT_FOR_COMPLETION_TAG = "?wait_for_completion=";

    private final IConfiguration config;

    @Inject
    public HttpModule(IConfiguration config)
    {
        this.config = config;
    }

    public String findMasterNodeURL()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(HTTP_TAG);
        builder.append(LOCAL_HOST);
        builder.append(URL_PORT_SEPARATOR);
        builder.append(config.getHttpPort());
        builder.append(MASTER_NODE_SUFFIX);
        return builder.toString();
    }

    public String runSnapshotBackupURL(String repositoryName, String snapshotName)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(HTTP_TAG);
        builder.append(LOCAL_HOST);
        builder.append(URL_PORT_SEPARATOR);
        builder.append(config.getHttpPort());
        builder.append(SNAPSHOT_BKP_KEYWORD);
        builder.append(repositoryName);
        builder.append(URL_PATH_SEPARATOR);
        builder.append(snapshotName);
        builder.append(SNAPSHOT_BKP_WAIT_FOR_COMPLETION_TAG);
        builder.append(config.waitForCompletionOfBackup());
        return builder.toString();
    }

    public String localhostURL()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(HTTP_TAG);
        builder.append(LOCAL_HOST);
        builder.append(URL_PORT_SEPARATOR);
        builder.append(config.getHttpPort());
        return builder.toString();
    }

}

