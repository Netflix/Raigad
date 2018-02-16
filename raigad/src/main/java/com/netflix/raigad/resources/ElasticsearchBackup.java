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
package com.netflix.raigad.resources;

import com.google.inject.Inject;
import com.netflix.raigad.backup.RestoreBackupManager;
import com.netflix.raigad.backup.SnapshotBackupManager;
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
    private static final String REST_RESTORE_RENAME_PATTERN = "rename_pattern";
    private static final String REST_RESTORE_RENAME_REPLACEMENT = "rename_replacement";

    private final SnapshotBackupManager snapshotBackupManager;
    private final RestoreBackupManager restoreBackupManager;

    @Inject
    public ElasticsearchBackup(SnapshotBackupManager snapshotBackupManager, RestoreBackupManager restoreBackupManager) {
        this.snapshotBackupManager = snapshotBackupManager;
        this.restoreBackupManager = restoreBackupManager;
    }

    @GET
    @Path("/do_snapshot")
    public Response snapshot() throws Exception {
        logger.info("Running snapshot through a REST call...");

        snapshotBackupManager.runSnapshotBackup();

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/do_restore")
    public Response restore(@QueryParam(REST_REPOSITORY_NAME) String repoName,
                            @QueryParam(REST_REPOSITORY_TYPE) String repoType,
                            @QueryParam(REST_SNAPSHOT_NAME) String snapName,
                            @QueryParam(REST_INDICES_NAME) String indicesName) throws Exception {
        logger.info("Running restore through a REST call...");

        restoreBackupManager.runRestore(repoName, repoType, snapName, indicesName, null, null);

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/do_restore_with_rename")
    public Response restoreWithRename(@QueryParam(REST_REPOSITORY_NAME) String repoName,
                                      @QueryParam(REST_REPOSITORY_TYPE) String repoType,
                                      @QueryParam(REST_SNAPSHOT_NAME) String snapName,
                                      @QueryParam(REST_INDICES_NAME) String indicesName,
                                      @QueryParam(REST_RESTORE_RENAME_PATTERN) String renamePattern,
                                      @QueryParam(REST_RESTORE_RENAME_REPLACEMENT) String renameReplacement) throws Exception {
        logger.info("Running Restore with rename through REST call ...");

        restoreBackupManager.runRestore(repoName, repoType, snapName, indicesName, renamePattern, renameReplacement);

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }
}
