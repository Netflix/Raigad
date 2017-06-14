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

package com.netflix.raigad.configuration;

import com.google.inject.ImplementedBy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@ImplementedBy(RaigadConfiguration.class)
public interface IConfiguration {

    void initialize();

    /**
     * @return Path to the home dir of Elasticsearch
     */
    String getElasticsearchHome();

    String getYamlLocation();

    String getBackupLocation();

    /**
     * @return Path to Elasticsearch startup script
     */
    String getElasticsearchStartupScript();

    /**
     * @return Path to Elasticsearch stop script
     */
    String getElasticsearchStopScript();

    int getTransportTcpPort();

    int getHttpPort();

    int getNumOfShards();

    int getNumOfReplicas();

    int getTotalShardsPerNode();

    String getRefreshInterval();

    boolean isMasterQuorumEnabled();

    int getMinimumMasterNodes();

    String getPingTimeout();

    boolean isPingMulticastEnabled();

    String getFdPingInterval();

    String getFdPingTimeout();

    /**
     * @return Location of the local data dir
     */
    String getDataFileLocation();

    /**
     * @return Location of the local log dir
     */
    String getLogFileLocation();

    boolean doesElasticsearchStartManually();

    /**
     * @return Cluster name
     */
    String getAppName();

    /**
     * @return RAC (or zone for AWS)
     */
    String getRac();

    /**
     * @return List of all RAC used for the cluster
     */
    List<String> getRacs();

    /**
     * @return Local hostmame
     */
    String getHostname();

    /**
     * @return Get instance name (for AWS)
     */
    String getInstanceName();

    /**
     * @return Get instance id (for AWS)
     */
    String getInstanceId();


    /**
     * @return Get the Data Center name (or region for AWS)
     */
    String getDC();

    /**
     * @param dc Set the current data center
     */
    void setDC(String dc);


    /**
     * Amazon specific setting to query ASG Membership
     */
    String getASGName();

    /**
     * Amazon specific setting to query ASG Membership
     */
    String getStackName();

    /**
     * Get the security group associated with nodes in this cluster
     */
    String getACLGroupName();


    /**
     * @return Get host IP
     */
    String getHostIP();

    /**
     * @return Get host Local IP
     */
    String getHostLocalIP();

    /**
     * @return Bootstrap cluster name (depends on another cass cluster)
     */
    String getBootClusterName();

    /**
     * @return Elasticsearch Process Name
     */
    String getElasticsearchProcessName();

    /**
     * @return Elasticsearch Discovery Type
     */
    String getElasticsearchDiscoveryType();

    /**
     * @return Whether it's a Multi-Region Setup
     */
    boolean isMultiDC();

    /**
     * @return Elasticsearch Index Refresh Interval
     */
    String getIndexRefreshInterval();

    String getClusterRoutingAttributes();

    boolean isAsgBasedDedicatedDeployment();

    boolean isCustomShardAllocationPolicyEnabled();

    String getClusterShardAllocationAttribute();

    /**
     * Providing a way to add New Config Params without any code change
     */
    String getExtraConfigParams();

    String getEsKeyName(String escarKey);

    boolean isDebugEnabled();

    boolean isShardPerNodeEnabled();

    boolean isIndexAutoCreationEnabled();

    String getIndexMetadata();

    int getAutoCreateIndexTimeout();

    int getAutoCreateIndexInitialStartDelaySeconds();

    int getAutoCreateIndexPeriodicScheduledHour();

    /*
        Backup related Config properties
    */

    boolean isSnapshotBackupEnabled();

    String getCommaSeparatedIndicesToBackup();

    boolean partiallyBackupIndices();

    boolean includeGlobalStateDuringBackup();

    boolean waitForCompletionOfBackup();

    boolean includeIndexNameInSnapshot();

    boolean isHourlySnapshotEnabled();

    long getBackupCronTimerInSeconds();

    /**
     * @return Backup hour for snapshot backups (0 - 23)
     */
    int getBackupHour();

    /*
        Restore related Config properties
     */

    boolean isRestoreEnabled();

    String getRestoreRepositoryName();

    String getRestoreSourceClusterName();

    String getRestoreSourceRepositoryRegion();

    String getRestoreLocation();

    String getRestoreRepositoryType();

    String getRestoreSnapshotName();

    String getCommaSeparatedIndicesToRestore();

    int getRestoreTaskInitialDelayInSeconds();

    boolean amITribeNode();

    boolean amIWriteEnabledTribeNode();

    boolean amIMetadataEnabledTribeNode();

    String getCommaSeparatedSourceClustersForTribeNode();

    boolean amISourceClusterForTribeNode();

    String getCommaSeparatedTribeClusterNames();

    boolean isNodeMismatchWithDiscoveryEnabled();

    int getDesiredNumberOfNodesInCluster();

    boolean isEurekaHealthCheckEnabled();

    boolean isLocalModeEnabled();

    String getCassandraKeyspaceName();

    int getCassandraThriftPortForAstyanax();

    boolean isEurekaHostSupplierEnabled();

    String getCommaSeparatedCassandraHostNames();

    boolean isSecutrityGroupInMultiDC();

    boolean isKibanaSetupRequired();

    int getKibanaPort();

    /**
     * @return Whether current cluster is Single Region cluster but is a Source Cluster in Multi-Region Tribe Node Setup
     */
    boolean amISourceClusterForTribeNodeInMultiDC();

    boolean reportMetricsFromMasterOnly();

    /**
     * To prefer the index from a specific tribe
     *
     * @return tribe id
     */
    String getTribePreferredClusterIdOnConflict();

    String getEsNodeName();

    /**
     * Parameters associated with VPC
     */

    /**
     * VPCMigration mode deals with moving instances from EC2 classic to VPC
     */
    boolean isVPCMigrationModeEnabled();

    /**
     * Check if instance is deployed in VPC
     *
     * @return true or false
     */
    boolean isDeployedInVPC();

    /**
     * Check if instance is deployed in VPC external
     *
     * @return true or false
     */
    boolean isVPCExternal();

    /**
     * Get the security group associated with nodes in this cluster in VPC
     */
    String getACLGroupNameForVPC();

    /**
     * Get the security group id for given Security Group in VPC
     */
    String getACLGroupIdForVPC();

    /**
     * Set the security group id for given Security Group in VPC
     */
    void setACLGroupIdForVPC(String aclGroupIdForVPC);

    /**
     * Get the MAC id for an instance
     */
    String getMacIdForInstance();
}