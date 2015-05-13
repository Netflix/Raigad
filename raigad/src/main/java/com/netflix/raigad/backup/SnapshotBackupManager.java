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
package com.netflix.raigad.backup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.scheduler.CronTimer;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import com.netflix.raigad.utils.*;
import com.netflix.servo.monitor.*;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class SnapshotBackupManager extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackupManager.class);
    public static String JOBNAME = "SnapshotBackupManager";
    private final AbstractRepository repository;
    private final HttpModule httpModule;
    private final AtomicInteger snapshotSuccess = new AtomicInteger(0);
    private final AtomicInteger snapshotFailure = new AtomicInteger(0);
    private static final AtomicBoolean isSnapshotRunning = new AtomicBoolean(false);
    private static final DateTimeZone currentZone = DateTimeZone.UTC;
    private static final String S3_REPO_FOLDER_DATE_FORMAT = "yyyyMMddHHmm";
    private static Timer snapshotDuration = new BasicTimer(MonitorConfig.builder("snapshotDuration").withTag("class","Elasticsearch_SnapshotBackupReporter").build(), TimeUnit.SECONDS);

    static {
        Monitors.registerObject(snapshotDuration);
    }
    @Inject
    public SnapshotBackupManager(IConfiguration config, @Named("s3")AbstractRepository repository, HttpModule httpModule) {
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
                if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
                    String exceptionMsg = "Elasticsearch is not yet started, hence not Starting Snapshot Operation";
                    logger.info(exceptionMsg);
                    return;
                }

                logger.info("Current node is the Master Node.");

                if (!config.isSnapshotBackupEnabled()) {
                    logger.info("Snapshot Backup is disabled, hence can not start Snapshot Backup.");
                    return;
                }

                //Run Snapshot Backup
                runSnapshotBackup();
            }
            else
            {
                if(config.isDebugEnabled())
                    logger.debug("Current node is not a Master Node yet, hence not running a Snapshot");
            }
        } catch (Exception e) {
            snapshotFailure.incrementAndGet();
            logger.warn("Exception thrown while running Snapshot Backup", e);
        }
    }

    public void runSnapshotBackup() throws Exception
    {
        // Create or Get Repository
        String repositoryName = repository.createOrGetSnapshotRepository();

        // StartBackup
        String snapshotName = getSnapshotName(config.getCommaSeparatedIndicesToBackup(), config.includeIndexNameInSnapshot());
        logger.info("Repository Name : <"+repositoryName+"> Snapshot Name : <"+snapshotName+"> \nRunning Snapshot now ... ");

        Client esTransportClient = ESTransportClient.instance(config).getTransportClient();

        Stopwatch snapshotTimer = snapshotDuration.start();
        //This is a blocking call. It'll wait until Snapshot is finished.
        CreateSnapshotResponse createSnapshotResponse =  getCreateSnapshotResponse(esTransportClient,repositoryName,snapshotName);

        logger.info("Snapshot Status = "+createSnapshotResponse.status().toString());
        if(createSnapshotResponse.status() == RestStatus.OK)
        {
            //TODO Add Servo Monitoring so that it can be verified from dashboard
            printSnapshotDetails(createSnapshotResponse);
            snapshotSuccess.incrementAndGet();
        }
        else if (createSnapshotResponse.status() == RestStatus.INTERNAL_SERVER_ERROR) {
            //TODO Add Servo Monitoring so that it can be verified from dashboard
            logger.info("Snapshot Completely Failed");
            snapshotFailure.incrementAndGet();
        }
        //Stop the timer
        snapshotTimer.stop();
    }

    //TODO: Map to Java Class and Create JSON
    public void printSnapshotDetails(CreateSnapshotResponse createSnapshotResponse)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Snapshot Details:");
        builder.append("\n\t Name = "+createSnapshotResponse.getSnapshotInfo().name());
        builder.append("\n\t Indices : ");
        for(String index:createSnapshotResponse.getSnapshotInfo().indices())
        {
            builder.append("\n\t\t Index = "+index);
        }
        builder.append("\n\t Start Time = "+createSnapshotResponse.getSnapshotInfo().startTime());
        builder.append("\n\t End Time = "+createSnapshotResponse.getSnapshotInfo().endTime());
        long minuteDuration =  (createSnapshotResponse.getSnapshotInfo().endTime() - createSnapshotResponse.getSnapshotInfo().startTime())/(1000*60);
        builder.append("\n\t Total Time Taken = " + minuteDuration + " Minutes");
        builder.append("\n\t Total Shards = "+createSnapshotResponse.getSnapshotInfo().totalShards());
        builder.append("\n\t Successful Shards = "+createSnapshotResponse.getSnapshotInfo().successfulShards());
        builder.append("\n\t Total Failed Shards = "+createSnapshotResponse.getSnapshotInfo().failedShards());

        if(createSnapshotResponse.getSnapshotInfo().failedShards() > 0)
        {
            for(SnapshotShardFailure failedShard:createSnapshotResponse.getSnapshotInfo().shardFailures())
            {
                builder.append("\n\t Failed Shards : ");
                builder.append("\n\t\t Index = " + failedShard.index());
                builder.append("\n\t\t Shard Id = "+failedShard.shardId());
                builder.append("\n\t\t Node Id = "+failedShard.nodeId());
                builder.append("\n\t\t Reason = "+failedShard.reason());
            }
        }

        logger.info(builder.toString());
    }

    public static TaskTimer getTimer(IConfiguration config)
    {
        if(config.isHourlySnapshotEnabled())
        {
           return new SimpleTimer(JOBNAME, config.getBackupCronTimerInSeconds() * 1000);
        }
        else {
           int hour = config.getBackupHour();
           return new CronTimer(hour, 1, 0);
        }
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public String getSnapshotName(String indices,boolean includeIndexNameInSnapshot) {
        StringBuilder snapshotName = new StringBuilder();
        if (includeIndexNameInSnapshot) {
            String indexName;
            if (indices.toLowerCase().equals("all"))
                indexName = "all";
            else
                indexName = StringUtils.replace(indices, ",", "_");
            snapshotName.append(indexName).append("_");
        }

        DateTime dt = new DateTime();
        DateTime dtGmt = dt.withZone(currentZone);
        String snapshotDate = SystemUtils.formatDate(dtGmt, S3_REPO_FOLDER_DATE_FORMAT);
        snapshotName.append(snapshotDate);
        return snapshotName.toString();
    }

    public int getNumSnapshotSuccess(){
        return snapshotSuccess.get();
    }

    public int getNumSnapshotFailure(){
        return snapshotFailure.get();
    }

    public CreateSnapshotResponse getCreateSnapshotResponse(Client esTransportClient,String repositoryName, String snapshotName)
    {
       return esTransportClient.admin().cluster().prepareCreateSnapshot(repositoryName, snapshotName)
               .setWaitForCompletion(config.waitForCompletionOfBackup())
               .setIndices(config.getCommaSeparatedIndicesToBackup())
               .setIncludeGlobalState(config.includeGlobalStateDuringBackup())
               .setPartial(config.partiallyBackupIndices()).get();
    }
    //                (esTransportClient.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state());//, equalTo(SnapshotState.SUCCESS));

    /*
                    NON-Blocking SnapshotRequest
                    ----------------------------
                    CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(repositoryName,snapshotName);
                    esTransportClient.admin().cluster().createSnapshot(createSnapshotRequest
                            .indices(config.getCommaSeparatedIndicesToBackup())
                            .includeGlobalState(config.includeGlobalStateDuringBackup())
                            .waitForCompletion(config.waitForCompletionOfBackup()), new ActionListener<CreateSnapshotResponse>() {
                        @Override
                        public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                            logger.info("Time take for Snapshot = ["+(createSnapshotResponse.getSnapshotInfo().endTime()-createSnapshotResponse.getSnapshotInfo().startTime())+"] Seconds");
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.info("Snapshot Completely Failed");
                        }
                    });
     */
}
