package com.netflix.elasticcar.aws;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.elasticcar.backup.SnapshotSettingsDO;
import com.netflix.elasticcar.backup.exception.CreateSnapshotException;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.objectmapper.DefaultSnapshotMapper;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;
import com.netflix.elasticcar.utils.SystemUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sloke on 7/1/14.
 */
@Singleton
public class SnapshotBackup extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    public static String JOBNAME = "SnapshotBackup";
    private final IConfiguration config;
    private final Provider<IRepository> repositoryProvider;
    private final DefaultSnapshotMapper defaultSnapshotMapper = new DefaultSnapshotMapper();
    private static final DateTimeZone currentZone = DateTimeZone.UTC;
    private static final String S3_REPO_FOLDER_DATE_FORMAT = "yyyyMMddHHmm";

    @Inject
    public SnapshotBackup(IConfiguration config, Provider<IRepository> repositoryProvider){
        super(config);
        this.config = config;
        this.repositoryProvider = repositoryProvider;
    }

    @Override
    public void execute() throws Exception
    {
        // Create Repository
        String repositoryName = repositoryProvider.get().createOrGetRepository(IRepository.RepositoryType.s3);


	/*
	 * ec2-50-19-28-170.compute-1.amazonaws.com:7104/_snapshot/20140320/snapshot_1?wait_for_completion=true
	 *
	 * {
     *  "indices": "index_1,index_2", //  "indices": "_all"
     *  "ignore_unavailable": "true",
     *  "include_global_state": false
     * }
	 *
	 */
        // StartBackup
        String snapshotName = getSnapshotName(config.getCommaSeparatedIndicesToBackup(),config.includeIndexNameInSnapshot()) ;
        //TODO: Make URL more robust eg. Use StringBuffer or something better
        String URL = "http://127.0.0.1:" + config.getHttpPort() + "/_snapshot/" + repositoryName + "/" + snapshotName + "?wait_for_completion=" + config.waitForCompletionOfBackup();
        SnapshotSettingsDO snapshotSettingsDO = new SnapshotSettingsDO(config.getCommaSeparatedIndicesToBackup(), config.ignoreUnavailableIndicesDuringBackup(), config.includeGlobalStateDuringBackup());
        String jsonBody = defaultSnapshotMapper.writeValueAsString(snapshotSettingsDO);
        if (config.isDebugEnabled())
            logger.info("Create Repository JSON : " + jsonBody);
        String response = SystemUtils.runHttpPutCommand(URL, jsonBody);
        if (response == null || response.isEmpty()) {
            logger.error("Response from URL : <" + URL + "> is Null or Empty, hence stopping the current running thread");
            throw new CreateSnapshotException("Response from URL : <" + URL + "> is Null or Empty, Creation of Snapshot failed !!");
        }
        logger.info("Response from URL : <" + URL + ">  = [" + response + "]. Successfully created a snapshot <" + snapshotName + ">");
    }

    @Override
    public String getName() {
        return JOBNAME;
    }


    public static TaskTimer getTimer(IConfiguration config)
    {
        //Remove after testing
        return new SimpleTimer(JOBNAME, 10L * 1000);

//        int hour = config.getBackupHour();
//        return new CronTimer(hour, 1, 0);
    }


    public static String getSnapshotName(String indices,boolean includeIndexNameInSnapshot) {
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

}
