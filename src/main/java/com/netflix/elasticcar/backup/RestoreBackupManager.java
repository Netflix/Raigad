package com.netflix.elasticcar.backup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.backup.exception.NoRepositoryException;
import com.netflix.elasticcar.backup.exception.RestoreBackupException;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;
import com.netflix.elasticcar.utils.ESTransportClient;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.elasticcar.utils.EsUtils;
import com.netflix.elasticcar.utils.HttpModule;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by sloke on 7/1/14.
 */
@Singleton
public class RestoreBackupManager extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(RestoreBackupManager.class);
    public static String JOBNAME = "RestoreBackupManager";
    private final IRepository repository;
    private final HttpModule httpModule;
    private static final AtomicBoolean isRestoreRunning = new AtomicBoolean(false);
    private static final String ALL_INDICES_TAG = "_all";

    @Inject
    public RestoreBackupManager(IConfiguration config, IRepository repository, HttpModule httpModule) {
        super(config);
        this.repository = repository;
        this.httpModule = httpModule;
    }

    @Override
    public void execute()
    {
        try {
            //Confirm if Current Node is a Master Node
            if (EsUtils.amIMasterNode(config,httpModule))
            {
                // If Elasticsearch is started then only start Snapshot Backup
                if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
                    String exceptionMsg = "Elasticsearch is not yet started, hence not Starting Restore Operation";
                    logger.info(exceptionMsg);
                    return;
                }

                logger.info("Current node is the Master Node. Running Restore now ...");
                runRestore(config.getRestoreRepositoryName(),
                        config.getRestoreRepositoryType(),
                        config.getRestoreSnapshotName(),
                        config.getCommaSeparatedIndicesToRestore());
            }
            else
            {
                logger.info("Current node is not a Master Node yet, hence not running a Restore");
            }
        } catch (Exception e) {
            logger.warn("Exception thrown while running Restore Backup", e);
        }
    }

    public void runRestore(String repositoryName, String repositoryType, String snapshotName, String indices) throws Exception
    {

        TransportClient esTransportClient = ESTransportClient.instance(config).getTransportClient();

        // Get Repository Name
        String repoN = (repositoryName == null || repositoryName.isEmpty()) ? config.getRestoreRepositoryName() : repositoryName;
        if(repoN == null || repoN.isEmpty())
            throw new RestoreBackupException("Repository Name is Null or Empty");

        String repoType = (repositoryType == null || repositoryType.isEmpty()) ? config.getRestoreRepositoryType().toLowerCase() : repositoryType;
        if(repoType == null || repoType.isEmpty())
        {
            logger.info("RepositoryType is empty, hence Defaulting to <s3> type");
            repoType = IRepository.RepositoryType.s3.name();
        }
        //TODO:Remove hard coded Type
        if(!repository.doesRepositoryExists(repoN, IRepository.RepositoryType.valueOf(repoType.toLowerCase())))
        {
           throw new NoRepositoryException("Repository <"+repositoryName+"> does not exist. Pick another repository.");
        }

        // Get Snapshot Name
        String snapshotN = (snapshotName == null || snapshotName.isEmpty()) ? config.getRestoreSnapshotName() : snapshotName;
        if(snapshotN == null || snapshotN.isEmpty())
        {
            //Pick the last Snapshot from the available Snapshots
            List<String> snapshots = EsUtils.getAvailableSnapshots(esTransportClient,repoN);
            if(snapshots.isEmpty())
                throw new RestoreBackupException("No available snapshots in <"+repoN+"> repository.");

            //Sorting Snapshot names in Reverse Order
            Collections.sort(snapshots,Collections.reverseOrder());

            //Use the Last available snapshot
            snapshotN = snapshots.get(0);
        }

        // Get Names of Indices
        String commaSeparatedIndices =  (indices == null || indices.isEmpty()) ? config.getCommaSeparatedIndicesToRestore() : indices;
        if(commaSeparatedIndices == null || commaSeparatedIndices.isEmpty() || commaSeparatedIndices.equalsIgnoreCase(ALL_INDICES_TAG))
            logger.info("Restoring all Indices. Param : <"+commaSeparatedIndices+">");

        //This is a blocking call. It'll wait until Restore is finished.
        RestoreSnapshotResponse restoreSnapshotResponse = esTransportClient.admin().cluster().prepareRestoreSnapshot(repoN, snapshotN)
                .setWaitForCompletion(true)
                .setIndices(commaSeparatedIndices)   //"test-idx-*", "-test-idx-2"
                .execute()
                .actionGet();

        logger.info("Restore Status = "+restoreSnapshotResponse.status().toString());
        if(restoreSnapshotResponse.status() == RestStatus.OK)
        {
            printRestoreDetails(restoreSnapshotResponse);
        }
        else if (restoreSnapshotResponse.status() == RestStatus.INTERNAL_SERVER_ERROR)
            logger.info("Snapshot Completely Failed");

    }

    //TODO: Map to Java Class and Create JSON
    public void printRestoreDetails(RestoreSnapshotResponse restoreSnapshotResponse)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Restore Details:");
        builder.append("\n\t Name = "+restoreSnapshotResponse.getRestoreInfo().name());
        builder.append("\n\t Indices : ");
        for(String index:restoreSnapshotResponse.getRestoreInfo().indices())
        {
            builder.append("\n\t\t Index = "+index);
        }
        builder.append("\n\t Total Shards = "+restoreSnapshotResponse.getRestoreInfo().totalShards());
        builder.append("\n\t Successful Shards = "+restoreSnapshotResponse.getRestoreInfo().successfulShards());
        builder.append("\n\t Total Failed Shards = "+restoreSnapshotResponse.getRestoreInfo().failedShards());

        logger.info(builder.toString());
    }

    public static TaskTimer getTimer(IConfiguration config)
    {
        return new SimpleTimer(JOBNAME);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

}
