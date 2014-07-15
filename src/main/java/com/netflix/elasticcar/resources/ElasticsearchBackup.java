package com.netflix.elasticcar.resources;

import com.google.inject.Inject;
import com.netflix.elasticcar.IElasticsearchProcess;
import com.netflix.elasticcar.backup.RestoreBackupManager;
import com.netflix.elasticcar.backup.SnapshotBackupManager;
import com.netflix.elasticcar.configuration.IConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/v1/esbackup")
@Produces(MediaType.APPLICATION_JSON)
public class ElasticsearchBackup
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchBackup.class);
    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final String REST_REPOSITORY_NAME = "repository";
    private static final String REST_SNAPSHOT_NAME = "snapshot";
    private static final String REST_INDICES_NAME = "indices";
    private static final String REST_REPOSITORY_TYPE = "type";
    private final IConfiguration config;
    private final IElasticsearchProcess esProcess;
    private final SnapshotBackupManager snapshotBackupManager;
    private final RestoreBackupManager restoreBackupManager;
    private static final String SHARD_REALLOCATION_PROPERTY = "cluster.routing.allocation.enable";

    @Inject
    public ElasticsearchBackup(IConfiguration config, IElasticsearchProcess esProcess,SnapshotBackupManager snapshotBackupManager,RestoreBackupManager restoreBackupManager)
    {
        this.config = config;
        this.esProcess = esProcess;
        this.snapshotBackupManager = snapshotBackupManager;
        this.restoreBackupManager = restoreBackupManager;
    }

    @GET
    @Path("/do_snapshot")
    public Response snapshot()
            throws IOException, InterruptedException, JSONException
    {
    	logger.info("Running Snapshot through REST call ...");
        snapshotBackupManager.execute();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/do_restore")
    public Response restore(@QueryParam(REST_REPOSITORY_NAME) String repoName,
                           @QueryParam(REST_SNAPSHOT_NAME) String snapName,
                           @QueryParam(REST_INDICES_NAME) String indicesName)
            throws Exception
    {
		logger.info("Running Restore through REST call ...");
        restoreBackupManager.runRestore(repoName,snapName,indicesName);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

}
