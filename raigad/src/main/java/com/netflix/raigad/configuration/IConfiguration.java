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
package com.netflix.raigad.configuration;

import com.google.inject.ImplementedBy;

import java.util.List;


@ImplementedBy(RaigadConfiguration.class)
public interface IConfiguration
{
	
    public void initialize();

    /**
     * @return Path to the home dir of Elasticsearch
     */
    public String getElasticsearchHome();

    public String getYamlLocation();

    public String getBackupLocation();

    /**
     * @return Path to Elasticsearch startup script
     */
    public String getElasticsearchStartupScript();

    /**
     * @return Path to Elasticsearch stop script
     */
    public String getElasticsearchStopScript();
   
    public int getTransportTcpPort();
    
    public int getHttpPort();

    public int getNumOfShards();
    
    public int getNumOfReplicas();

    public int getTotalShardsPerNode();

    public String getRefreshInterval();
    
    public boolean isMasterQuorumEnabled();
    
    public int getMinimumMasterNodes();
    
    public String getPingTimeout();
    
    public boolean isPingMulticastEnabled();
    
    public String getFdPingInterval();
    
    public String getFdPingTimeout();   

    /**
     * @return Location of the local data dir
     */
	public String getDataFileLocation();

    /**
     * @return Location of the local log dir
     */
	public String getLogFileLocation();

	public boolean doesElasticsearchStartManually();

    /**
     * @return Cluster name
     */
    public String getAppName();

    /**
     * @return RAC (or zone for AWS)
     */
    public String getRac();

    /**
     * @return List of all RAC used for the cluster
     */
    public List<String> getRacs();

    /**
     * @return Local hostmame
     */
    public String getHostname();

    /**
     * @return Get instance name (for AWS)
     */
    public String getInstanceName();

    /**
     * @return Get instance id (for AWS)
     */
    public String getInstanceId();


    /**
     * @return Get the Data Center name (or region for AWS)
     */
    public String getDC();

    /**
     * @param dc
     *            Set the current data center
     */
    public void setDC(String dc);

 
    /**
     * Amazon specific setting to query ASG Membership
     */
    public String getASGName();
    
    /**
     * Get the security group associated with nodes in this cluster
     */
    public String getACLGroupName();

   
    /**
     * @return Get host Public IP
     */
    public String getHostIP();

    /**
     * @return Get host Local IP
     */
    public String getHostLocalIP();
   
    /**
     * @return Bootstrap cluster name (depends on another cass cluster)
     */
    public String getBootClusterName();
    
    /**
     * @return Elasticsearch Process Name
     */
    public String getElasticsearchProcessName();

    /**
     * @return Elasticsearch Discovery Type
     */
    public String getElasticsearchDiscoveryType();

    /**
     * @return Whether it's a Multi-Region Setup
     */
	public boolean isMultiDC();

    /**
     * @return Elasticsearch Index Refresh Interval
     */
	public String getIndexRefreshInterval();

    public String getClusterRoutingAttributes();
    
    public boolean isAsgBasedDedicatedDeployment();

    public boolean isCustomShardAllocationPolicyEnabled();

    public String getClusterShardAllocationAttribute();

    /**
     * Providing a way to add New Config Params without any code change
     */
    public String getExtraConfigParams();

    public String getEsKeyName(String escarKey);

    public boolean isDebugEnabled();

    public boolean isShardPerNodeEnabled();

    public boolean isIndexAutoCreationEnabled();

    public String getIndexMetadata();

    public int getAutoCreateIndexTimeout();

    public int getAutoCreateIndexInitialStartDelaySeconds();

    public int getAutoCreateIndexPeriodicScheduledHour();

    /*
        Backup related Config properties
    */

    public boolean isSnapshotBackupEnabled();

    public String getCommaSeparatedIndicesToBackup();

    public boolean partiallyBackupIndices();

    public boolean includeGlobalStateDuringBackup();

    public boolean waitForCompletionOfBackup();

    public boolean includeIndexNameInSnapshot();

    public boolean isHourlySnapshotEnabled();

    public long getBackupCronTimerInSeconds();

    /**
     * @return Backup hour for snapshot backups (0 - 23)
     */
    public int getBackupHour();

    /*
        Restore related Config properties
     */

    public boolean isRestoreEnabled();

    public String getRestoreRepositoryName();

    public String getRestoreSourceClusterName();

    public String getRestoreSourceRepositoryRegion();

    public String getRestoreLocation();

    public String getRestoreRepositoryType();

    public String getRestoreSnapshotName();

    public String getCommaSeparatedIndicesToRestore();

    public int getRestoreTaskInitialDelayInSeconds();

    public boolean amITribeNode();

    public boolean amIWriteEnabledTribeNode();

    public boolean amIMetadataEnabledTribeNode();

    public String getCommaSeparatedSourceClustersForTribeNode();

    public boolean amISourceClusterForTribeNode();

    public String getCommaSeparatedTribeClusterNames();

    public boolean isNodeMismatchWithDiscoveryEnabled();

    public int getDesiredNumberOfNodesInCluster();

    public boolean isEurekaHealthCheckEnabled();

    public boolean isLocalModeEnabled();

    public String getCassandraKeyspaceName();

    public int getCassandraThriftPortForAstyanax();

    public boolean isEurekaHostSupplierEnabled();

    public String getCommaSeparatedCassandraHostNames();

    public boolean isSecutrityGroupInMultiDC();

    public boolean isKibanaSetupRequired();

    public int getKibanaPort();

    /**
     * @return Whether current cluster is Single Region cluster but is a Source Cluster in Multi-Region Tribe Node Setup
     */
    public boolean amISourceClusterForTribeNodeInMultiDC();

    public boolean reportMetricsFromMasterOnly();

    /**
     * To prefer the index from a specific tribe
     * @return tribe id
     */
    public String getTribePreferredClusterIdOnConflict();
}
