/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.netflix.raigad.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;


@Singleton
public class HttpModule {
    private static final String HTTP_TAG = "http://";
    private static final String LOCAL_HOST = "127.0.0.1";
    private static final String URL_PORT_SEPARATOR = ":";
    private static final String URL_PATH_SEPARATOR = "/";
    private static final String MASTER_NODE_SUFFIX = "/_cat/master?h=ip";
    private static final String SNAPSHOT_BKP_KEYWORD = "/_snapshot/";
    private static final String SNAPSHOT_BKP_WAIT_FOR_COMPLETION_TAG = "?wait_for_completion=";

    private final IConfiguration config;

    @Inject
    public HttpModule(IConfiguration config) {
        this.config = config;
    }

    public String findMasterNodeURL() {
        StringBuilder builder = new StringBuilder();
        builder.append(HTTP_TAG);
        builder.append(LOCAL_HOST);
        builder.append(URL_PORT_SEPARATOR);
        builder.append(config.getHttpPort());
        builder.append(MASTER_NODE_SUFFIX);

        return builder.toString();
    }

    public String runSnapshotBackupURL(String repositoryName, String snapshotName) {
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
}

