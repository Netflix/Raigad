package com.netflix.raigad.configuration;

import java.util.List;

public class FakeConfiguration implements IConfiguration {

    public static final String FAKE_REGION = "us-east-1";
    public static final String INDEX_METADATA = "[    {        \"retentionType\": \"daily\",        \"retentionPeriod\": 5,    \"indexName\": \"test_index\", \"preCreate\": \"true\"   }]";

    public String region;
    public String appName;
    public String zone;
    public String instance_id;

    public FakeConfiguration()
    {
        this(FAKE_REGION, "my_fake_cluster", "my_zone", "i-01234567");
    }

    public FakeConfiguration(String region, String appName, String zone, String ins_id)
    {
        this.region = region;
        this.appName = appName;
        this.zone = zone;
        this.instance_id = ins_id;
    }

    @Override
    public void initialize() {

    }

    @Override
    public String getElasticsearchHome() {
        return null;
    }

    @Override
    public String getYamlLocation() {
        return null;
    }

    @Override
    public String getBackupLocation() {
        return "es-backup-test";
    }

    @Override
    public String getElasticsearchStartupScript() {
        return null;
    }

    @Override
    public String getElasticsearchStopScript() {
        return null;
    }

    @Override
    public int getTransportTcpPort() {
        return 0;
    }

    @Override
    public int getHttpPort() {
        return 0;
    }

    @Override
    public int getNumOfShards() {
        return 0;
    }

    @Override
    public int getNumOfReplicas() {
        return 0;
    }

    @Override
    public int getTotalShardsPerNode() {
        return 0;
    }

    @Override
    public String getRefreshInterval() {
        return null;
    }

    @Override
    public boolean isMasterQuorumEnabled() {
        return false;
    }

    @Override
    public int getMinimumMasterNodes() {
        return 0;
    }

    @Override
    public String getPingTimeout() {
        return null;
    }

    @Override
    public boolean isPingMulticastEnabled() {
        return false;
    }

    @Override
    public String getFdPingInterval() {
        return null;
    }

    @Override
    public String getFdPingTimeout() {
        return null;
    }

    @Override
    public String getDataFileLocation() {
        return null;
    }

    @Override
    public String getLogFileLocation() {
        return null;
    }

    @Override
    public boolean doesElasticsearchStartManually() {
        return false;
    }

    @Override
    public String getAppName() {
        return appName;
    }

    @Override
    public String getRac() {
        return null;
    }

    @Override
    public List<String> getRacs() {
        return null;
    }

    @Override
    public String getHostname() {
        return null;
    }

    @Override
    public String getInstanceName() {
        return null;
    }

    @Override
    public String getInstanceId() {
        return null;
    }

    @Override
    public String getDC() {
        return "us-east-1";
    }

    @Override
    public void setDC(String dc) {

    }

    @Override
    public String getASGName() {
        return null;
    }

    @Override
    public String getACLGroupName() {
        return null;
    }

    @Override
    public String getHostIP() {
        return null;
    }

    @Override
    public String getHostLocalIP() {
        return null;
    }

    @Override
    public String getBootClusterName() {
        return null;
    }

    @Override
    public String getElasticsearchProcessName() {
        return null;
    }

    @Override
    public String getElasticsearchDiscoveryType() {
        return null;
    }

    @Override
    public boolean isMultiDC() {
        return false;
    }

    @Override
    public String getIndexRefreshInterval() {
        return null;
    }

    @Override
    public String getClusterRoutingAttributes() {
        return null;
    }

    @Override
    public boolean isAsgBasedDedicatedDeployment() {
        return false;
    }

    @Override
    public boolean isCustomShardAllocationPolicyEnabled() {
        return false;
    }

    @Override
    public String getClusterShardAllocationAttribute() {
        return null;
    }

    @Override
    public String getExtraConfigParams() {
        return null;
    }

    @Override
    public String getEsKeyName(String escarKey) {
        return null;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isShardPerNodeEnabled() {
        return false;
    }

    @Override
    public boolean isIndexAutoCreationEnabled() {
        return false;
    }

    @Override
    public String getIndexMetadata() {
        return INDEX_METADATA;
    }

    @Override
    public int getAutoCreateIndexTimeout() {
        return 3000;
    }

    @Override
    public int getAutoCreateIndexInitialStartDelaySeconds() {
        return 0;
    }

    @Override
    public int getAutoCreateIndexPeriodicScheduledHour() {
        return 0;
    }

    @Override
    public boolean isSnapshotBackupEnabled() {
        return false;
    }

    @Override
    public String getCommaSeparatedIndicesToBackup() {
        return "_all";
    }

    @Override
    public boolean partiallyBackupIndices() {
        return false;
    }

    @Override
    public boolean includeGlobalStateDuringBackup() {
        return false;
    }

    @Override
    public boolean waitForCompletionOfBackup() {
        return true;
    }

    @Override
    public boolean includeIndexNameInSnapshot() {
        return false;
    }

    @Override
    public boolean isHourlySnapshotEnabled() {
        return false;
    }

    @Override
    public long getBackupCronTimerInSeconds() {
        return 0;
    }

    @Override
    public int getBackupHour() {
        return 0;
    }

    @Override
    public boolean isRestoreEnabled() {
        return false;
    }

    @Override
    public String getRestoreRepositoryName() {
        return null;
    }

    @Override
    public String getRestoreSourceClusterName() {
        return "fake-app";
    }

    @Override
    public String getRestoreSourceRepositoryRegion() {
        return null;
    }

    @Override
    public String getRestoreLocation() {
        return null;
    }

    @Override
    public String getRestoreRepositoryType() {
        return null;
    }

    @Override
    public String getRestoreSnapshotName() {
        return null;
    }

    @Override
    public String getCommaSeparatedIndicesToRestore() {
        return null;
    }

    @Override
    public int getRestoreTaskInitialDelayInSeconds() {
        return 0;
    }

    @Override
    public boolean amITribeNode() {
        return false;
    }

    @Override
    public boolean amIWriteEnabledTribeNode() {
        return false;
    }

    @Override
    public boolean amIMetadataEnabledTribeNode() {
        return false;
    }

    @Override
    public String getCommaSeparatedSourceClustersForTribeNode() {
        return null;
    }

    @Override
    public boolean amISourceClusterForTribeNode() {
        return false;
    }

    @Override
    public String getCommaSeparatedTribeClusterNames() {
        return null;
    }

    @Override
    public boolean isNodeMismatchWithDiscoveryEnabled() {
        return false;
    }

    @Override
    public int getDesiredNumberOfNodesInCluster() {
        return 0;
    }

    @Override
    public boolean isEurekaHealthCheckEnabled() {
        return false;
    }

    @Override
    public boolean isLocalModeEnabled() {
        return false;
    }

    @Override
    public String getCassandraKeyspaceName() {
        return null;
    }

    @Override
    public int getCassandraThriftPortForAstyanax() {
        return 0;
    }

    @Override
    public boolean isEurekaHostSupplierEnabled() {
        return false;
    }

    @Override
    public String getCommaSeparatedCassandraHostNames() {
        return null;
    }

    @Override
    public boolean isSecutrityGroupInMultiDC() {
        return false;
    }

    @Override
    public boolean amISourceClusterForTribeNodeInMultiDC() {
        return false;
    }

}
