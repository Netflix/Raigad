/**
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.raigad.resources;

import com.google.inject.Inject;
import com.netflix.raigad.defaultimpl.IElasticsearchProcess;
import com.netflix.raigad.backup.RestoreBackupManager;
import com.netflix.raigad.backup.SnapshotBackupManager;
import com.netflix.raigad.configuration.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/esbackup")
@Produces(MediaType.APPLICATION_JSON)
public class ElasticsearchBackup {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchBackup.class);
    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final String REST_REPOSITORY_NAME = "repository_name";
    private static final String REST_REPOSITORY_TYPE = "repository_type";
    private static final String REST_SNAPSHOT_NAME = "snapshot";
    private static final String REST_INDICES_NAME = "indices";
    private final IConfiguration config;
    private final IElasticsearchProcess esProcess;
    private final SnapshotBackupManager snapshotBackupManager;
    private final RestoreBackupManager restoreBackupManager;
    private static final String SHARD_REALLOCATION_PROPERTY = "cluster.routing.allocation.enable";

    @Inject
    public ElasticsearchBackup(IConfiguration config, IElasticsearchProcess esProcess, SnapshotBackupManager snapshotBackupManager, RestoreBackupManager restoreBackupManager) {
        this.config = config;
        this.esProcess = esProcess;
        this.snapshotBackupManager = snapshotBackupManager;
        this.restoreBackupManager = restoreBackupManager;
    }

    @GET
    @Path("/do_snapshot")
    public Response snapshot()
            throws Exception {
        logger.info("Running Snapshot through REST call ...");
        snapshotBackupManager.runSnapshotBackup();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/do_restore")
    public Response restore(@QueryParam(REST_REPOSITORY_NAME) String repoName,
                            @QueryParam(REST_REPOSITORY_TYPE) String repoType,
                            @QueryParam(REST_SNAPSHOT_NAME) String snapName,
                            @QueryParam(REST_INDICES_NAME) String indicesName)
            throws Exception {
        logger.info("Running Restore through REST call ...");
        restoreBackupManager.runRestore(repoName, repoType, snapName, indicesName);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

}
