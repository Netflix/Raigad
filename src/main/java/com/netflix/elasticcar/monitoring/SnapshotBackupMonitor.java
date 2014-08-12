package com.netflix.elasticcar.monitoring;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.backup.SnapshotBackupManager;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by sloke on 7/16/14.
 */
@Singleton
public class SnapshotBackupMonitor extends Task
{

    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackupMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_SnapshotBackupMonitor";
    private final Elasticsearch_SnapshotBackupReporter snapshotBackupReporter;
    private final SnapshotBackupManager snapshotBackupManager;

    @Inject
    public SnapshotBackupMonitor(IConfiguration config,SnapshotBackupManager snapshotBackupManager)
    {
        super(config);
        snapshotBackupReporter = new Elasticsearch_SnapshotBackupReporter();
        this.snapshotBackupManager = snapshotBackupManager;
        Monitors.registerObject(snapshotBackupReporter);
    }

    @Override
    public void execute() throws Exception {

        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        SnapshotBackupBean snapshotBackupBean = new SnapshotBackupBean();
        try
        {
            snapshotBackupBean.snapshotSuccess = snapshotBackupManager.getNumSnapshotSuccess();
            snapshotBackupBean.snapshotFailure = snapshotBackupManager.getNumSnapshotFailure();
        }
        catch(Exception e)
        {
            logger.warn("failed to load Cluster SnapshotBackup Status", e);
        }

        snapshotBackupReporter.snapshotBackupBean.set(snapshotBackupBean);
    }

    public class Elasticsearch_SnapshotBackupReporter
    {
        private final AtomicReference<SnapshotBackupBean> snapshotBackupBean;

        public Elasticsearch_SnapshotBackupReporter()
        {
            snapshotBackupBean = new AtomicReference<SnapshotBackupBean>(new SnapshotBackupBean());
        }

        @Monitor(name="snapshot_success", type= DataSourceType.GAUGE)
        public int getSnapshotSuccess() {
            return snapshotBackupBean.get().snapshotSuccess;
        }

        @Monitor(name="snapshot_failure", type=DataSourceType.GAUGE)
        public int getSnapshotFailure() {
            return snapshotBackupBean.get().snapshotFailure;
        }

    }

    private static class SnapshotBackupBean
    {
        private int snapshotSuccess = -1;
        private int snapshotFailure = -1;
    }

    public static TaskTimer getTimer(String name)
    {
        return new SimpleTimer(name, 3600 * 1000);
    }

    @Override
    public String getName()
    {
        return METRIC_NAME;
    }

}
