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

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.*;
import com.netflix.raigad.aws.ICredential;
import com.netflix.raigad.utils.RetryableCallable;
import com.netflix.raigad.utils.SystemUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Singleton
public class RaigadConfiguration implements IConfiguration
{
    public static final String MY_WEBAPP_NAME = "Raigad";

    private static final String CONFIG_CLUSTER_NAME = MY_WEBAPP_NAME + ".es.clustername";
    private static final String CONFIG_AVAILABILITY_ZONES = MY_WEBAPP_NAME + ".zones.available";
    private static final String CONFIG_DATA_LOCATION = MY_WEBAPP_NAME + ".es.data.location";
    private static final String CONFIG_LOG_LOCATION = MY_WEBAPP_NAME + ".es.log.location";
    private static final String CONFIG_ES_START_SCRIPT = MY_WEBAPP_NAME + ".es.startscript";
    private static final String CONFIG_YAML_LOCATION = MY_WEBAPP_NAME + ".es.yamlLocation";
    private static final String CONFIG_ES_STOP_SCRIPT = MY_WEBAPP_NAME + ".es.stopscript";
    private static final String CONFIG_ES_HOME = MY_WEBAPP_NAME + ".es.home";
    private static final String CONFIG_FD_PING_INTERVAL = MY_WEBAPP_NAME + ".es.fd.pinginterval";
    private static final String CONFIG_FD_PING_TIMEOUT = MY_WEBAPP_NAME + ".es.fd.pingtimeout";
    private static final String CONFIG_HTTP_PORT = MY_WEBAPP_NAME + ".es.http.port";
    private static final String CONFIG_TRANSPORT_TCP_PORT = MY_WEBAPP_NAME + ".es.transport.tcp.port";
    private static final String CONFIG_MIN_MASTER_NODES = MY_WEBAPP_NAME + ".es.min.master.nodes";
    private static final String CONFIG_NUM_REPLICAS = MY_WEBAPP_NAME + ".es.num.replicas";
    private static final String CONFIG_NUM_SHARDS = MY_WEBAPP_NAME + ".es.num.shards";
    private static final String CONFIG_PING_TIMEOUT = MY_WEBAPP_NAME + ".es.pingtimeout";
    private static final String CONFIG_INDEX_REFRESH_INTERVAL = MY_WEBAPP_NAME + ".es.index.refresh.interval";
    private static final String CONFIG_IS_MASTER_QUORUM_ENABLED = MY_WEBAPP_NAME + ".es.master.quorum.enabled";
    private static final String CONFIG_IS_PING_MULTICAST_ENABLED = MY_WEBAPP_NAME + ".es.ping.multicast.enabled";
    private static final String CONFIG_ES_DISCOVERY_TYPE = MY_WEBAPP_NAME + ".es.discovery.type";
    private static final String CONFIG_BOOTCLUSTER_NAME = MY_WEBAPP_NAME + ".bootcluster";
    private static final String CONFIG_INSTANCE_DATA_RETRIEVER = MY_WEBAPP_NAME + ".instanceDataRetriever";
    private static final String CONFIG_CREDENTIAL_PROVIDER = MY_WEBAPP_NAME + ".credentialProvider";
    private static final String CONFIG_SECURITY_GROUP_NAME = MY_WEBAPP_NAME + ".security.group.name";
    private static final String CONFIG_IS_MULTI_DC_ENABLED = MY_WEBAPP_NAME + ".es.multi.dc.enabled";
    private static final String CONFIG_IS_ASG_BASED_DEPLOYMENT_ENABLED = MY_WEBAPP_NAME + ".es.asg.based.deployment.enabled";
    private static final String CONFIG_ES_CLUSTER_ROUTING_ATTRIBUTES = MY_WEBAPP_NAME + ".es.cluster.routing.attributes";
    private static final String CONFIG_ES_PROCESS_NAME = MY_WEBAPP_NAME + ".es.processname";
    private static final String CONFIG_ES_SHARD_ALLOCATION_ATTRIBUTE = MY_WEBAPP_NAME + ".es.shard.allocation.attribute";
    private static final String CONFIG_IS_SHARD_ALLOCATION_POLICY_ENABLED = MY_WEBAPP_NAME + ".shard.allocation.policy.enabled";
    private static final String CONFIG_EXTRA_PARAMS = MY_WEBAPP_NAME + ".extra.params";
    private static final String CONFIG_IS_DEBUG_ENABLED = MY_WEBAPP_NAME + ".debug.enabled";
    private static final String CONFIG_IS_SHARDS_PER_NODE_ENABLED = MY_WEBAPP_NAME + ".shards.per.node.enabled";
    private static final String CONFIG_SHARDS_PER_NODE = MY_WEBAPP_NAME + ".shards.per.node";
    private static final String CONFIG_INDEX_METADATA = MY_WEBAPP_NAME + ".index.metadata";
    private static final String CONFIG_IS_INDEX_AUTOCREATION_ENABLED = MY_WEBAPP_NAME + ".index.autocreation.enabled";
    private static final String CONFIG_AUTOCREATE_INDEX_TIMEOUT = MY_WEBAPP_NAME + ".autocreate.index.timeout";
    private static final String CONFIG_AUTOCREATE_INDEX_INITIAL_START_DELAY_SECONDS = MY_WEBAPP_NAME + ".autocreate.index.initial.start.delay.seconds";
    private static final String CONFIG_AUTOCREATE_INDEX_PERIODIC_SCHEDULED_HOUR = MY_WEBAPP_NAME + ".autocreate.index.periodic.scheduled.delay.seconds";
    private static final String CONFIG_BACKUP_LOCATION = MY_WEBAPP_NAME + ".backup.location";
    private static final String CONFIG_BACKUP_HOUR = MY_WEBAPP_NAME + ".backup.hour";
    private static final String CONFIG_BACKUP_IS_SNAPSHOT_ENABLED = MY_WEBAPP_NAME + ".snapshot.enabled";
    private static final String CONFIG_BACKUP_IS_HOURLY_SNAPSHOT_ENABLED = MY_WEBAPP_NAME + ".hourly.snapshot.enabled";
    private static final String CONFIG_BACKUP_COMMA_SEPARATED_INDICES = MY_WEBAPP_NAME + ".backup.comma.separated.indices";
    private static final String CONFIG_BACKUP_PARTIAL_INDICES = MY_WEBAPP_NAME + ".backup.partial.indices";
    private static final String CONFIG_BACKUP_INCLUDE_GLOBAL_STATE = MY_WEBAPP_NAME + ".backup.include.global.state";
    private static final String CONFIG_BACKUP_WAIT_FOR_COMPLETION = MY_WEBAPP_NAME + ".backup.wait.for.completion";
    private static final String CONFIG_BACKUP_INCLUDE_INDEX_NAME = MY_WEBAPP_NAME + ".backup.include.index.name";
    private static final String CONFIG_BACKUP_CRON_TIMER_SECONDS = MY_WEBAPP_NAME + ".backup.cron.timer.seconds";
    private static final String CONFIG_IS_RESTORE_ENABLED = MY_WEBAPP_NAME + ".restore.enabled";
    private static final String CONFIG_RESTORE_REPOSITORY_NAME = MY_WEBAPP_NAME + ".restore.repository.name";
    private static final String CONFIG_RESTORE_REPOSITORY_TYPE = MY_WEBAPP_NAME + ".restore.repository.type";
    private static final String CONFIG_RESTORE_SNAPSHOT_NAME = MY_WEBAPP_NAME + ".restore.snapshot.name";
    private static final String CONFIG_RESTORE_COMMA_SEPARATED_INDICES = MY_WEBAPP_NAME + ".restore.comma.separated.indices";
    private static final String CONFIG_RESTORE_TASK_INITIAL_START_DELAY_SECONDS = MY_WEBAPP_NAME + ".restore.task.initial.start.delay.seconds";
    private static final String CONFIG_RESTORE_SOURCE_CLUSTER_NAME = MY_WEBAPP_NAME + ".restore.source.cluster.name";
    private static final String CONFIG_RESTORE_SOURCE_REPO_REGION = MY_WEBAPP_NAME + ".restore.source.repo.region";
    private static final String CONFIG_RESTORE_LOCATION = MY_WEBAPP_NAME + ".restore.location";
    private static final String CONFIG_AM_I_TRIBE_NODE = MY_WEBAPP_NAME + ".tribe.node.enabled";
    private static final String CONFIG_AM_I_WRITE_ENABLED_TRIBE_NODE = MY_WEBAPP_NAME + ".tribe.node.write.enabled";
    private static final String CONFIG_AM_I_METADATA_ENABLED_TRIBE_NODE = MY_WEBAPP_NAME + ".tribe.node.metadata.enabled";
    private static final String CONFIG_TRIBE_COMMA_SEPARATED_SOURCE_CLUSTERS = MY_WEBAPP_NAME + ".tribe.comma.separated.source.clusters";
    private static final String CONFIG_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE = MY_WEBAPP_NAME + ".tribe.node.source.cluster.enabled";
    private static final String CONFIG_TRIBE_COMMA_SEPARATED_TRIBE_CLUSTERS = MY_WEBAPP_NAME + ".tribe.comma.separated.tribe.clusters";
    private static final String CONFIG_IS_NODEMISMATCH_WITH_DISCOVERY_ENABLED = MY_WEBAPP_NAME + ".nodemismatch.health.metrics.enabled";
    private static final String CONFIG_DESIRED_NUM_NODES_IN_CLUSTER = MY_WEBAPP_NAME + ".desired.num.nodes.in.cluster";
    private static final String CONFIG_IS_EUREKA_HEALTH_CHECK_ENABLED = MY_WEBAPP_NAME + ".eureka.health.check.enabled";
    private static final String CONFIG_IS_LOCAL_MODE_ENABLED = MY_WEBAPP_NAME + ".local.mode.enabled";
    private static final String CONFIG_CASSANDRA_KEYSPACE_NAME = MY_WEBAPP_NAME + ".cassandra.keyspace.name";
    private static final String CONFIG_CASSANDRA_THRIFT_PORT = MY_WEBAPP_NAME + ".cassandra.thrift.port";
    private static final String CONFIG_IS_EUREKA_HOST_SUPPLIER_ENABLED = MY_WEBAPP_NAME + ".eureka.host.supplier.enabled";
    private static final String CONFIG_COMMA_SEPARATED_CASSANDRA_HOSTNAMES = MY_WEBAPP_NAME + ".comma.separated.cassandra.hostnames";
    private static final String CONFIG_IS_SECURITY_GROUP_IN_MULTI_DC = MY_WEBAPP_NAME + ".security.group.in.multi.dc.enabled";
    private static final String CONFIG_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE_IN_MULTI_DC = MY_WEBAPP_NAME + ".tribe.node.source.cluster.enabled.in.multi.dc";
    private static final String CONFIG_REPORT_METRICS_FROM_MASTER_ONLY = MY_WEBAPP_NAME + ".report.metrics.from.master.only";

    // Amazon specific
    private static final String CONFIG_ASG_NAME = MY_WEBAPP_NAME + ".az.asgname";
    private static final String CONFIG_REGION_NAME = MY_WEBAPP_NAME + ".az.region";
    private static final String CONFIG_ACL_GROUP_NAME = MY_WEBAPP_NAME + ".acl.groupname";

    private static final String RAC = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/placement/availability-zone");
    private static final String PUBLIC_HOSTNAME = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-hostname").trim();
    private static final String PUBLIC_IP = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-ipv4").trim();
    private static final String LOCAL_HOSTNAME = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-hostname").trim();
    private static final String LOCAL_IP = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/local-ipv4").trim();
    private static final String INSTANCE_ID = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-id").trim();
    private static final String INSTANCE_TYPE = SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-type").trim();

    private static String ASG_NAME = System.getenv("ASG_NAME");
    private static String REGION = System.getenv("EC2_REGION");

    // Defaults
    private final String DEFAULT_CLUSTER_NAME = "es_samplecluster";
    private final String DEFAULT_ES_HOME_DIR = "/apps/elasticsearch";
    private List<String> DEFAULT_AVAILABILITY_ZONES = ImmutableList.of();


    private static final String DEFAULT_DATA_LOCATION = "/mnt/data/es";
    private static final String DEFAULT_LOG_LOCATION = "/logs/es";
    private static final String DEFAULT_YAML_LOCATION = "/apps/elasticsearch/config/elasticsearch.yml";
    private static final String DEFAULT_ES_START_SCRIPT = "/etc/init.d/elasticsearch start";
    private static final String DEFAULT_ES_STOP_SCRIPT = "/etc/init.d/elasticsearch stop";

    private static final String DEFAULT_ES_HOME = "/apps/elasticsearch";
    private static final String DEFAULT_FD_PING_INTERVAL = "60s";
    private static final String DEFAULT_FD_PING_TIMEOUT = "60s";
    private static final int DEFAULT_HTTP_PORT = 7104;
    private static final int DEFAULT_TRANSPORT_TCP_PORT = 7102;
    private static final int DEFAULT_MIN_MASTER_NODES = 1;
    private static final int DEFAULT_NUM_REPLICAS = 2;
    private static final int DEFAULT_NUM_SHARDS = 5;
    private static final String DEFAULT_PING_TIMEOUT = "60s";
    private static final String DEFAULT_INDEX_REFRESH_INTERVAL = "1m";
    private static final boolean DEFAULT_IS_MASTER_QUORUM_ENABLED = true;
    private static final boolean DEFAULT_IS_PING_MULTICAST_ENABLED = false;
    private static final String DEFAULT_CONFIG_BOOTCLUSTER_NAME = "cass_metadata";
    private static final String DEFAULT_CREDENTIAL_PROVIDER = "com.netflix.raigad.aws.IAMCredential";
    private static final String DEFAULT_ES_DISCOVERY_TYPE = "custom";
    private static final boolean DEFAULT_IS_MULTI_DC_ENABLED = false;
    private static final boolean DEFAULT_IS_ASG_BASED_DEPLOYMENT_ENABLED = false;
    private static final String DEFAULT_ES_CLUSTER_ROUTING_ATTRIBUTES = "rack_id";
    private static final String DEFAULT_ES_PROCESS_NAME = "org.elasticsearch.bootstrap.Elasticsearch";
    private static final boolean DEFAULT_IS_SHARD_ALLOCATION_POLICY_ENABLED = false;
    private static final String DEFAULT_ES_SHARD_ALLOCATION_ATTRIBUTE = "all";
    private static final String DEFAULT_CONFIG_EXTRA_PARAMS = null;
    private static final boolean DEFAULT_IS_DEBUG_ENABLED = false;
    private static final boolean DEFAULT_IS_SHARDS_PER_NODE_ENABLED = false;
    private static final int DEFAULT_SHARDS_PER_NODE = 5;
    private static final boolean DEFAULT_IS_INDEX_AUTOCREATION_ENABLED = false;
    private static final int DEFAULT_AUTOCREATE_INDEX_TIMEOUT = 300000;
    private static final int DEFAULT_AUTOCREATE_INDEX_INITIAL_START_DELAY_SECONDS = 300;
    private static final int DEFAULT_AUTOCREATE_INDEX_PERIODIC_SCHEDULED_HOUR = 22;
    private static final String DEFAULT_INDEX_METADATA = null;
    private static final String DEFAULT_BACKUP_LOCATION = "elasticsearch-us-east-1-backup";
    private static final int DEFAULT_BACKUP_HOUR = 1;
    private static final String DEFAULT_BACKUP_COMMA_SEPARATED_INDICES = "_all";
    private static final boolean DEFAULT_BACKUP_PARTIAL_INDICES = false;
    private static final boolean DEFAULT_BACKUP_INCLUDE_GLOBAL_STATE = false;
    private static final boolean DEFAULT_BACKUP_WAIT_FOR_COMPLETION = true;
    private static final boolean DEFAULT_BACKUP_INCLUDE_INDEX_NAME = false;
    private static final boolean DEFAULT_IS_RESTORE_ENABLED = false;
    private static final String DEFAULT_RESTORE_REPOSITORY_NAME = "testrepo";
    private static final String DEFAULT_RESTORE_REPOSITORY_TYPE = "s3";
    private static final String DEFAULT_RESTORE_SNAPSHOT_NAME = "";
    private static final String DEFAULT_RESTORE_COMMA_SEPARATED_INDICES = "_all";
    private static final int DEFAULT_RESTORE_TASK_INITIAL_START_DELAY_SECONDS = 600;
    private static final String DEFAULT_RESTORE_SOURCE_CLUSTER_NAME = "";
    private static final String DEFAULT_RESTORE_SOURCE_REPO_REGION = "us-east-1";
    private static final String DEFAULT_RESTORE_LOCATION = "elasticsearch-us-east-1-backup";
    private static final boolean DEFAULT_BACKUP_IS_SNAPSHOT_ENABLED = false;
    private static final boolean DEFAULT_BACKUP_IS_HOURLY_SNAPSHOT_ENABLED = false;
    private static final long DEFAULT_BACKUP_CRON_TIMER_SECONDS = 3600;
    private static final boolean DEFAULT_AM_I_TRIBE_NODE = false;
    private static final boolean DEFAULT_AM_I_WRITE_ENABLED_TRIBE_NODE = false;
    private static final boolean DEFAULT_AM_I_METADATA_ENABLED_TRIBE_NODE = false;
    private static final String DEFAULT_TRIBE_COMMA_SEPARATED_SOURCE_CLUSTERS = "";
    private static final boolean DEFAULT_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE = false;
    private static final String DEFAULT_TRIBE_COMMA_SEPARATED_TRIBE_CLUSTERS = "";
    private static final boolean DEFAULT_IS_NODEMISMATCH_WITH_DISCOVERY_ENABLED = false;
    private static final int DEFAULT_DESIRED_NUM_NODES_IN_CLUSTER = 6;
    private static final boolean DEFAULT_IS_EUREKA_HEALTH_CHECK_ENABLED = true;
    private static final boolean DEFAULT_IS_LOCAL_MODE_ENABLED = false;
    private static final String DEFAULT_CASSANDRA_KEYSPACE_NAME = "escarbootstrap";
    private static final int DEFAULT_CASSANDRA_THRIFT_PORT = 7102;
    private static final boolean DEFAULT_IS_EUREKA_HOST_SUPPLIER_ENABLED = true;
    private static final String DEFAULT_COMMA_SEPARATED_CASSANDRA_HOSTNAMES = "";
    private static final boolean DEFAULT_IS_SECURITY_GROUP_IN_MULTI_DC = false;
    private static final boolean DEFAULT_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE_IN_MULTI_DC = false;
    private static final boolean DEFAULT_REPORT_METRICS_FROM_MASTER_ONLY = false;

    private final IConfigSource config; 
    private static final Logger logger = LoggerFactory.getLogger(RaigadConfiguration.class);
    private final ICredential provider;

    private final DynamicStringProperty CREDENTIAL_PROVIDER = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_CREDENTIAL_PROVIDER, getDefaultCredentialProvider());
    private final DynamicStringProperty ES_STARTUP_SCRIPT_LOCATION = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_ES_START_SCRIPT, getDefaultEsStartScript());
    private final DynamicStringProperty ES_STOP_SCRIPT_LOCATION = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_ES_STOP_SCRIPT, getDefaultEsStopScript());
    private final DynamicStringProperty DATA_LOCATION = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_DATA_LOCATION, getDefaultDataLocation());
    private final DynamicStringProperty LOG_LOCATION = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_LOG_LOCATION, getDefaultLogLocation());
    private final DynamicStringProperty YAML_LOCATION = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_YAML_LOCATION, getDefaultYamlLocation());
    private final DynamicStringProperty ES_HOME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_ES_HOME, getDefaultEsHome());
    private final DynamicStringProperty FD_PING_INTERVAL = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_FD_PING_INTERVAL,getDefaultFdPingInterval());
    private final DynamicStringProperty FD_PING_TIMEOUT = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_FD_PING_TIMEOUT,getDefaultFdPingTimeout());
    private final DynamicIntProperty ES_HTTP_PORT = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_HTTP_PORT, getDefaultHttpPort());
    private final DynamicIntProperty ES_TRANSPORT_TCP_PORT = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_TRANSPORT_TCP_PORT, getDefaultTransportTcpPort());
    private final DynamicIntProperty MINIMUM_MASTER_NODES = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_MIN_MASTER_NODES, getDefaultMinMasterNodes());
    private final DynamicIntProperty NUM_REPLICAS = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_NUM_REPLICAS, getDefaultNumReplicas());
    private final DynamicIntProperty NUM_SHARDS = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_NUM_SHARDS, getDefaultNumShards());
    private final DynamicStringProperty PING_TIMEOUT = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_PING_TIMEOUT, getDefaultPingTimeout());
    private final DynamicStringProperty INDEX_REFRESH_INTERVAL = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_INDEX_REFRESH_INTERVAL, getDefaultIndexRefreshInterval());
    private final DynamicBooleanProperty IS_MASTER_QUORUM_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_MASTER_QUORUM_ENABLED, isDefaultIsMasterQuorumEnabled());
    private final DynamicBooleanProperty IS_PING_MULTICAST_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_PING_MULTICAST_ENABLED, isDefaultIsPingMulticastEnabled());
    private final DynamicStringProperty BOOTCLUSTER_NAME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_BOOTCLUSTER_NAME, getDefaultConfigBootclusterName());
    private final DynamicStringProperty ES_DISCOVERY_TYPE = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_ES_DISCOVERY_TYPE, getDefaultEsDiscoveryType());
    private final DynamicStringProperty SECURITY_GROUP_NAME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_SECURITY_GROUP_NAME, getAppName());
    private final DynamicBooleanProperty IS_MULTI_DC_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_MULTI_DC_ENABLED, isDefaultIsMultiDcEnabled());
    private final DynamicBooleanProperty IS_ASG_BASED_DEPLOYMENT_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_ASG_BASED_DEPLOYMENT_ENABLED, isDefaultIsAsgBasedDeploymentEnabled());
    private final DynamicStringProperty ES_CLUSTER_ROUTING_ATTRIBUTES = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_ES_CLUSTER_ROUTING_ATTRIBUTES, getDefaultEsClusterRoutingAttributes());
    private final DynamicBooleanProperty IS_SHARD_ALLOCATION_POLICY_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_SHARD_ALLOCATION_POLICY_ENABLED, isDefaultIsShardAllocationPolicyEnabled());
    private final DynamicStringProperty ES_SHARD_ALLOCATION_ATTRIBUTE = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_ES_SHARD_ALLOCATION_ATTRIBUTE, getDefaultEsShardAllocationAttribute());
    private final DynamicStringProperty EXTRA_PARAMS = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_EXTRA_PARAMS, getDefaultConfigExtraParams());
    private final DynamicBooleanProperty IS_DEBUG_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_DEBUG_ENABLED, isDefaultIsDebugEnabled());
    private final DynamicBooleanProperty IS_SHARDS_PER_NODE_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_SHARDS_PER_NODE_ENABLED, isDefaultIsShardsPerNodeEnabled());
    private final DynamicIntProperty TOTAL_SHARDS_PER_NODES = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_SHARDS_PER_NODE, getDefaultShardsPerNode());
    private final DynamicStringProperty INDEX_METADATA = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_INDEX_METADATA, getDefaultIndexMetadata());
    private final DynamicBooleanProperty IS_INDEX_AUTOCREATION_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_INDEX_AUTOCREATION_ENABLED, isDefaultIsIndexAutocreationEnabled());
    private final DynamicIntProperty AUTOCREATE_INDEX_TIMEOUT = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_AUTOCREATE_INDEX_TIMEOUT, getDefaultAutocreateIndexTimeout());
    private final DynamicIntProperty AUTOCREATE_INDEX_INITIAL_START_DELAY_SECONDS = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_AUTOCREATE_INDEX_INITIAL_START_DELAY_SECONDS,getDefaultAutocreateIndexInitialStartDelaySeconds());
    private final DynamicIntProperty AUTOCREATE_INDEX_PERIODIC_SCHEDULED_HOUR = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_AUTOCREATE_INDEX_PERIODIC_SCHEDULED_HOUR, getDefaultAutocreateIndexPeriodicScheduledHour());
    private final DynamicStringProperty ES_PROCESS_NAME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_ES_PROCESS_NAME, getDefaultEsProcessName());
    private final DynamicStringProperty BUCKET_NAME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_BACKUP_LOCATION, getDefaultBackupLocation());
    private final DynamicIntProperty BACKUP_HOUR = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_BACKUP_HOUR, getDefaultBackupHour());
    private final DynamicStringProperty COMMA_SEPARATED_INDICES_TO_BACKUP = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_BACKUP_COMMA_SEPARATED_INDICES, getDefaultBackupCommaSeparatedIndices());
    private final DynamicBooleanProperty PARTIALLY_BACKUP_INDICES = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_BACKUP_PARTIAL_INDICES, isDefaultBackupPartialIndices());
    private final DynamicBooleanProperty INCLUDE_GLOBAL_STATE_DURING_BACKUP = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_BACKUP_INCLUDE_GLOBAL_STATE, isDefaultBackupIncludeGlobalState());
    private final DynamicBooleanProperty WAIT_FOR_COMPLETION_OF_BACKUP = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_BACKUP_WAIT_FOR_COMPLETION, isDefaultBackupWaitForCompletion());
    private final DynamicBooleanProperty INCLUDE_INDEX_NAME_IN_SNAPSHOT_BACKUP = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_BACKUP_INCLUDE_INDEX_NAME, isDefaultBackupIncludeIndexName());
    private final DynamicBooleanProperty IS_RESTORE_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_RESTORE_ENABLED, isDefaultIsRestoreEnabled());
    private final DynamicStringProperty RESTORE_REPOSITORY_NAME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_RESTORE_REPOSITORY_NAME, getDefaultRestoreRepositoryName());
    private final DynamicStringProperty RESTORE_REPOSITORY_TYPE = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_RESTORE_REPOSITORY_TYPE, getDefaultRestoreRepositoryType());
    private final DynamicStringProperty RESTORE_SNAPSHOT_NAME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_RESTORE_SNAPSHOT_NAME, getDefaultRestoreSnapshotName());
    private final DynamicStringProperty COMMA_SEPARATED_INDICES_TO_RESTORE = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_RESTORE_COMMA_SEPARATED_INDICES, getDefaultRestoreCommaSeparatedIndices());
    private final DynamicIntProperty RESTORE_TASK_INITIAL_START_DELAY_SECONDS = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_RESTORE_TASK_INITIAL_START_DELAY_SECONDS, getDefaultRestoreTaskInitialStartDelaySeconds());
    private final DynamicStringProperty RESTORE_SOURCE_CLUSTER_NAME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_RESTORE_SOURCE_CLUSTER_NAME, getDefaultRestoreSourceClusterName());
    private final DynamicStringProperty RESTORE_SOURCE_REPO_REGION = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_RESTORE_SOURCE_REPO_REGION, getDefaultRestoreSourceRepoRegion());
    private final DynamicStringProperty RESTORE_LOCATION = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_RESTORE_LOCATION, getDefaultRestoreLocation());
    private final DynamicBooleanProperty IS_SNAPSHOT_BACKUP_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_BACKUP_IS_SNAPSHOT_ENABLED, isDefaultBackupIsSnapshotEnabled());
    private final DynamicBooleanProperty IS_HOURLY_SNAPSHOT_BACKUP_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_BACKUP_IS_HOURLY_SNAPSHOT_ENABLED, isDefaultBackupIsHourlySnapshotEnabled());
    private final DynamicLongProperty BACKUP_CRON_TIMER_SECONDS = DynamicPropertyFactory.getInstance().getLongProperty(CONFIG_BACKUP_CRON_TIMER_SECONDS, getDefaultBackupCronTimerSeconds());
    private final DynamicBooleanProperty AM_I_TRIBE_NODE = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_AM_I_TRIBE_NODE, isDefaultAmITribeNode());
    private final DynamicBooleanProperty AM_I_WRITE_ENABLED_TRIBE_NODE = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_AM_I_WRITE_ENABLED_TRIBE_NODE, isDefaultAmIWriteEnabledTribeNode());
    private final DynamicBooleanProperty AM_I_METADATA_ENABLED_TRIBE_NODE = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_AM_I_METADATA_ENABLED_TRIBE_NODE, isDefaultAmIMetadataEnabledTribeNode());
    private final DynamicStringProperty COMMA_SEPARATED_SOURCE_CLUSTERS_IN_TRIBE = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_TRIBE_COMMA_SEPARATED_SOURCE_CLUSTERS, getDefaultTribeCommaSeparatedSourceClusters());
    private final DynamicBooleanProperty AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE, isDefaultAmISourceClusterForTribeNode());
    private final DynamicStringProperty COMMA_SEPARATED_TRIBE_CLUSTERS = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_TRIBE_COMMA_SEPARATED_TRIBE_CLUSTERS, getDefaultTribeCommaSeparatedTribeClusters());
    private final DynamicBooleanProperty IS_NODE_MISMATCH_WITH_DISCOVERY_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_NODEMISMATCH_WITH_DISCOVERY_ENABLED, isDefaultIsNodemismatchWithDiscoveryEnabled());
    private final DynamicIntProperty DESIRED_NUM_NODES_IN_CLUSTER = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_DESIRED_NUM_NODES_IN_CLUSTER, getDefaultDesiredNumNodesInCluster());
    private final DynamicBooleanProperty IS_EUREKA_HEALTH_CHECK_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_EUREKA_HEALTH_CHECK_ENABLED, isDefaultIsEurekaHealthCheckEnabled());
    private final DynamicBooleanProperty IS_LOCAL_MODE_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_LOCAL_MODE_ENABLED, isDefaultIsLocalModeEnabled());
    private final DynamicStringProperty CASSANDRA_KEYSPACE_NAME = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_CASSANDRA_KEYSPACE_NAME, getDefaultCassandraKeyspaceName());
    private final DynamicIntProperty CASSANDRA_THRIFT_PORT = DynamicPropertyFactory.getInstance().getIntProperty(CONFIG_CASSANDRA_THRIFT_PORT, getDefaultCassandraThriftPort());
    private final DynamicBooleanProperty IS_EUREKA_HOST_SUPPLIER_ENABLED = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_EUREKA_HOST_SUPPLIER_ENABLED, isDefaultIsEurekaHostSupplierEnabled());
    private final DynamicStringProperty COMMA_SEPARATED_CASSANDRA_HOSTNAMES = DynamicPropertyFactory.getInstance().getStringProperty(CONFIG_COMMA_SEPARATED_CASSANDRA_HOSTNAMES, getDefaultCommaSeparatedCassandraHostnames());
    private final DynamicBooleanProperty IS_SECURITY_GROUP_IN_MULTI_DC = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_IS_SECURITY_GROUP_IN_MULTI_DC, isDefaultIsSecurityGroupInMultiDc());
    private final DynamicBooleanProperty AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE_IN_MULTI_DC = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE_IN_MULTI_DC, isDefaultAmISourceClusterForTribeNodeInMultiDC());
    private final DynamicBooleanProperty REPORT_METRICS_FROM_MASTER_ONLY = DynamicPropertyFactory.getInstance().getBooleanProperty(CONFIG_REPORT_METRICS_FROM_MASTER_ONLY, getDefaultReportMetricsFromMasterOnly());


    @Inject
    public RaigadConfiguration(ICredential provider, IConfigSource config)
    {
        this.provider = provider;
        this.config = config;
    }

    @Override
    public void initialize()
    {
        setupEnvVars();
        this.config.initialize(ASG_NAME, REGION);
        setDefaultRACList(REGION);
        populateProps();
        SystemUtils.createDirs(getDataFileLocation());
    }

    private void setupEnvVars()
    {
        // Search in java opt properties
        REGION = StringUtils.isBlank(REGION) ? System.getProperty("EC2_REGION") : REGION;
        // Infer from zone
        if (StringUtils.isBlank(REGION))
            REGION = RAC.substring(0, RAC.length() - 1);
        ASG_NAME = StringUtils.isBlank(ASG_NAME) ? System.getProperty("ASG_NAME") : ASG_NAME;
        if (StringUtils.isBlank(ASG_NAME))
            ASG_NAME = populateASGName(REGION, INSTANCE_ID);
        logger.info(String.format("REGION set to %s, ASG Name set to %s", REGION, ASG_NAME));
    }

    /**
     * Query amazon to get ASG name. Currently not available as part of instance
     * info api.
     */
    private String populateASGName(String region, String instanceId)
    {
        GetASGName getASGName = new GetASGName(region, instanceId);
        
        try {
            return getASGName.call();
        } catch (Exception e) {
            logger.error("Failed to determine ASG name.", e);
            return null;
        }
    }
    
    private class GetASGName extends RetryableCallable<String>
    {
        private static final int NUMBER_OF_RETRIES = 15;
        private static final long WAIT_TIME = 30000;
        private final String region;
        private final String instanceId;
        private final AmazonEC2 client;
        
        public GetASGName(String region, String instanceId) {
            super(NUMBER_OF_RETRIES, WAIT_TIME);
            this.region = region;
            this.instanceId = instanceId;
            client = new AmazonEC2Client(provider.getAwsCredentialProvider());
            client.setEndpoint("ec2." + region + ".amazonaws.com");
        }
        
        @Override
        public String retriableCall() throws IllegalStateException {
            DescribeInstancesRequest desc = new DescribeInstancesRequest().withInstanceIds(instanceId);
            DescribeInstancesResult res = client.describeInstances(desc);
    
            for (Reservation resr : res.getReservations())
            {
                for (Instance ins : resr.getInstances())
                {
                    for (com.amazonaws.services.ec2.model.Tag tag : ins.getTags())
                    {
                        if (tag.getKey().equals("aws:autoscaling:groupName"))
                            return tag.getValue();
                    }
                }
            }
            
            logger.warn("Couldn't determine ASG name");
            throw new IllegalStateException("Couldn't determine ASG name");
        }
    }

    /**
     * Get the fist 3 available zones in the region
     */
    public void setDefaultRACList(String region){
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + region + ".amazonaws.com");
        DescribeAvailabilityZonesResult res = client.describeAvailabilityZones();
        List<String> zone = Lists.newArrayList();
        for(AvailabilityZone reg : res.getAvailabilityZones()){
            if( reg.getState().equals("available") )
                zone.add(reg.getZoneName());
            if( zone.size() == 3)
                break;
        }
//        DEFAULT_AVAILABILITY_ZONES =  StringUtils.join(zone, ",");
      DEFAULT_AVAILABILITY_ZONES = ImmutableList.copyOf(zone);
    }

    private void populateProps()
    {
        config.set(CONFIG_ASG_NAME, ASG_NAME);
        config.set(CONFIG_REGION_NAME, REGION);
    }

    @Override
    public List<String> getRacs()
    {
        return config.getList(CONFIG_AVAILABILITY_ZONES, DEFAULT_AVAILABILITY_ZONES);
    }
   

    @Override
    public String getDC()
    {
        return config.get(CONFIG_REGION_NAME, "");
    }

    @Override
    public void setDC(String region)
    {
        config.set(CONFIG_REGION_NAME, region);
    }

    
    @Override
    public String getASGName()
    {
        return config.get(CONFIG_ASG_NAME, ASG_NAME);
    }

    @Override
    public String getACLGroupName()
    {
    	return config.get(CONFIG_ACL_GROUP_NAME, this.getAppName());
    }

    @Override
    public String getDataFileLocation() {
        return DATA_LOCATION.get();
    }

    @Override
    public String getLogFileLocation() {
        return LOG_LOCATION.get();
    }

    @Override
    public String getElasticsearchStartupScript() {
        return ES_STARTUP_SCRIPT_LOCATION.get();
    }

    @Override
    public String getYamlLocation()
    {
        return YAML_LOCATION.get();
    }

    @Override
    public String getBackupLocation() {
        return BUCKET_NAME.get();
    }

    @Override
    public String getElasticsearchHome() {
        return ES_HOME.get();
    }

    @Override
    public String getElasticsearchStopScript() {
        return ES_STOP_SCRIPT_LOCATION.get();
    }

    @Override
    public String getFdPingInterval() {
        return FD_PING_INTERVAL.get();
    }

    @Override
    public String getFdPingTimeout() {
        return FD_PING_TIMEOUT.get();
    }

    @Override
    public int getHttpPort() {
        return ES_HTTP_PORT.get();
    }

    @Override
    public int getTransportTcpPort() {
        return ES_TRANSPORT_TCP_PORT.get();
    }

    @Override
    public int getMinimumMasterNodes() {
        return MINIMUM_MASTER_NODES.get();
    }

    @Override
    public int getNumOfReplicas() {
        return NUM_REPLICAS.get();
    }

    @Override
    public int getTotalShardsPerNode() {
        return TOTAL_SHARDS_PER_NODES.get();
    }

    @Override
    public int getNumOfShards() {
        return NUM_SHARDS.get();
    }

    @Override
    public String getPingTimeout() {
        return PING_TIMEOUT.get();
    }

    @Override
    public String getRefreshInterval() {
        return INDEX_REFRESH_INTERVAL.get();
    }

    @Override
    public boolean isMasterQuorumEnabled() {
        return IS_MASTER_QUORUM_ENABLED.get();
    }

    @Override
    public boolean isPingMulticastEnabled() {
        return IS_PING_MULTICAST_ENABLED.get();
    }

    @Override
    public String getHostIP() {
        return PUBLIC_IP;
    }

    @Override
    public String getHostname() {
        return PUBLIC_HOSTNAME;
    }

    @Override
    public String getInstanceName() {
        return INSTANCE_ID;
    }

    @Override
    public String getInstanceId() {
        return INSTANCE_ID;
    }

    @Override
    public String getHostLocalIP() {
        return LOCAL_IP;
    }

    @Override
    public String getRac() {
        return RAC;
    }

    @Override
    public String getAppName() {
        return config.get(CONFIG_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    }

    @Override
    public String getBootClusterName() {
        return BOOTCLUSTER_NAME.get();
    }

    @Override
    public String getElasticsearchDiscoveryType() {
        return ES_DISCOVERY_TYPE.get();
    }

    @Override
    public boolean isMultiDC() {
        return IS_MULTI_DC_ENABLED.get();
    }

    @Override
    public String getClusterRoutingAttributes() {
        return ES_CLUSTER_ROUTING_ATTRIBUTES.get();
    }

    @Override
    public boolean isAsgBasedDedicatedDeployment() {
        return IS_ASG_BASED_DEPLOYMENT_ENABLED.get();
    }

    @Override
    public String getElasticsearchProcessName() {
        return ES_PROCESS_NAME.get();
    }

    /**
     * @return Elasticsearch Index Refresh Interval
     */
    public String getIndexRefreshInterval()
    {
        return INDEX_REFRESH_INTERVAL.get();
    }


    @Override
    public boolean doesElasticsearchStartManually() {
        return false;
    }

    @Override
    public String getClusterShardAllocationAttribute() {
        return ES_SHARD_ALLOCATION_ATTRIBUTE.get();
    }

    @Override
    public boolean isCustomShardAllocationPolicyEnabled() {
        return IS_SHARD_ALLOCATION_POLICY_ENABLED.get();
    }

    @Override
    public String getEsKeyName(String escarKey) {
        return config.get(escarKey);
    }

    @Override
    public boolean isDebugEnabled() {
        return IS_DEBUG_ENABLED.get();
    }

    @Override
    public boolean isShardPerNodeEnabled() {
        return IS_SHARDS_PER_NODE_ENABLED.get();
    }

    @Override
    public boolean isIndexAutoCreationEnabled() {
        return IS_INDEX_AUTOCREATION_ENABLED.get();
    }

    @Override
    public String getIndexMetadata() {
        return INDEX_METADATA.get();
    }

    @Override
    public int getAutoCreateIndexTimeout() {
        return AUTOCREATE_INDEX_TIMEOUT.get();
    }

    @Override
    public int getAutoCreateIndexInitialStartDelaySeconds() {
        return AUTOCREATE_INDEX_INITIAL_START_DELAY_SECONDS.get();
    }

    @Override
    public int getAutoCreateIndexPeriodicScheduledHour() {
        return AUTOCREATE_INDEX_PERIODIC_SCHEDULED_HOUR.get();
    }

    @Override
    public String getExtraConfigParams() {
        return EXTRA_PARAMS.get();
    }

    @Override
    public int getBackupHour() {
        return BACKUP_HOUR.get();
    }

    public boolean isSnapshotBackupEnabled(){
        return IS_SNAPSHOT_BACKUP_ENABLED.get();
    }

    @Override
    public String getCommaSeparatedIndicesToBackup() {
        return COMMA_SEPARATED_INDICES_TO_BACKUP.get();
    }

    @Override
    public boolean partiallyBackupIndices() {
        return PARTIALLY_BACKUP_INDICES.get();
    }

    @Override
    public boolean includeGlobalStateDuringBackup() {
        return INCLUDE_GLOBAL_STATE_DURING_BACKUP.get();
    }

    @Override
    public boolean waitForCompletionOfBackup() {
        return WAIT_FOR_COMPLETION_OF_BACKUP.get();
    }

    @Override
    public boolean includeIndexNameInSnapshot() {
        return INCLUDE_INDEX_NAME_IN_SNAPSHOT_BACKUP.get();
    }

    @Override
    public boolean isHourlySnapshotEnabled() {
        return IS_HOURLY_SNAPSHOT_BACKUP_ENABLED.get();
    }

    @Override
    public long getBackupCronTimerInSeconds() {
        return BACKUP_CRON_TIMER_SECONDS.get();
    }

    @Override
    public boolean isRestoreEnabled() {
        return IS_RESTORE_ENABLED.get();
    }

    @Override
    public String getRestoreRepositoryName() {
        return RESTORE_REPOSITORY_NAME.get();
    }

    @Override
    public String getRestoreSourceClusterName() {
        return RESTORE_SOURCE_CLUSTER_NAME.get();
    }

    @Override
    public String getRestoreSourceRepositoryRegion() {
        return RESTORE_SOURCE_REPO_REGION.get();
    }

    @Override
    public String getRestoreLocation() {
        return RESTORE_LOCATION.get();
    }

    @Override
    public String getRestoreRepositoryType() {
        return RESTORE_REPOSITORY_TYPE.get();
    }

    @Override
    public String getRestoreSnapshotName() {
        return RESTORE_SNAPSHOT_NAME.get();
    }

    @Override
    public String getCommaSeparatedIndicesToRestore() {
        return COMMA_SEPARATED_INDICES_TO_RESTORE.get();
    }

    @Override
    public int getRestoreTaskInitialDelayInSeconds() {
        return RESTORE_TASK_INITIAL_START_DELAY_SECONDS.get();
    }

    @Override
    public boolean amITribeNode() {
        return AM_I_TRIBE_NODE.get();
    }

    @Override
    public boolean amIWriteEnabledTribeNode() {
        return AM_I_WRITE_ENABLED_TRIBE_NODE.get();
    }

    @Override
    public boolean amIMetadataEnabledTribeNode() {
        return AM_I_METADATA_ENABLED_TRIBE_NODE.get();
    }

    @Override
    public String getCommaSeparatedSourceClustersForTribeNode() {
        return COMMA_SEPARATED_SOURCE_CLUSTERS_IN_TRIBE.get();
    }

    @Override
    public boolean amISourceClusterForTribeNode() {
        return AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE.get();
    }

    @Override
    public String getCommaSeparatedTribeClusterNames() {
        return COMMA_SEPARATED_TRIBE_CLUSTERS.get();
    }

    @Override
    public boolean isNodeMismatchWithDiscoveryEnabled() {
        return IS_NODE_MISMATCH_WITH_DISCOVERY_ENABLED.get();
    }

    @Override
    public int getDesiredNumberOfNodesInCluster() {
        return DESIRED_NUM_NODES_IN_CLUSTER.get();
    }

    @Override
    public boolean isEurekaHealthCheckEnabled() {
        return IS_EUREKA_HEALTH_CHECK_ENABLED.get();
    }

    @Override
    public boolean isLocalModeEnabled() {
        return IS_LOCAL_MODE_ENABLED.get();
    }

    @Override
    public String getCassandraKeyspaceName() {
        return CASSANDRA_KEYSPACE_NAME.get();
    }

    @Override
    public int getCassandraThriftPortForAstyanax() {
        return CASSANDRA_THRIFT_PORT.get();
    }

    @Override
    public boolean isEurekaHostSupplierEnabled() {
        return IS_EUREKA_HOST_SUPPLIER_ENABLED.get();
    }

    @Override
    public String getCommaSeparatedCassandraHostNames() {
        return COMMA_SEPARATED_CASSANDRA_HOSTNAMES.get();
    }

    @Override
    public boolean isSecutrityGroupInMultiDC() {
        return IS_SECURITY_GROUP_IN_MULTI_DC.get();
    }

    @Override
    public boolean amISourceClusterForTribeNodeInMultiDC() {
        return AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE_IN_MULTI_DC.get();
    }

    @Override
    public boolean reportMetricsFromMasterOnly() {
        return REPORT_METRICS_FROM_MASTER_ONLY.get();
    }

    public String getDefaultCredentialProvider()
    {
       return config.get(CONFIG_CREDENTIAL_PROVIDER,DEFAULT_CREDENTIAL_PROVIDER);
    }

    public String getDefaultEsStartScript()
    {
        return config.get(CONFIG_ES_START_SCRIPT,DEFAULT_ES_START_SCRIPT);
    }

    public String getDefaultEsStopScript()
    {
        return config.get(CONFIG_ES_STOP_SCRIPT,DEFAULT_ES_STOP_SCRIPT);
    }

    public String getDefaultDataLocation()
    {
        return config.get(CONFIG_DATA_LOCATION,DEFAULT_DATA_LOCATION);
    }

    public String getDefaultLogLocation()
    {
        return config.get(CONFIG_LOG_LOCATION,DEFAULT_LOG_LOCATION);
    }

    public String getDefaultYamlLocation()
    {
        return config.get(CONFIG_YAML_LOCATION,DEFAULT_YAML_LOCATION);
    }

    public String getDefaultEsHome()
    {
        return config.get(CONFIG_ES_HOME,DEFAULT_ES_HOME);
    }

    public String getDefaultFdPingInterval()
    {
        return config.get(CONFIG_FD_PING_INTERVAL,DEFAULT_FD_PING_INTERVAL);
    }

    public String getDefaultFdPingTimeout()
    {
        return config.get(CONFIG_FD_PING_TIMEOUT,DEFAULT_FD_PING_TIMEOUT);
    }

    public int getDefaultHttpPort()
    {
        return config.get(CONFIG_HTTP_PORT,DEFAULT_HTTP_PORT);
    }

    public int getDefaultTransportTcpPort()
    {
        return config.get(CONFIG_TRANSPORT_TCP_PORT,DEFAULT_TRANSPORT_TCP_PORT);
    }

    public int getDefaultMinMasterNodes()
    {
        return config.get(CONFIG_MIN_MASTER_NODES,DEFAULT_MIN_MASTER_NODES);
    }

    public int getDefaultNumReplicas()
    {
        return config.get(CONFIG_NUM_REPLICAS,DEFAULT_NUM_REPLICAS);
    }

    public int getDefaultNumShards()
    {
        return config.get(CONFIG_NUM_SHARDS,DEFAULT_NUM_SHARDS);
    }

    public String getDefaultPingTimeout()
    {
        return config.get(CONFIG_PING_TIMEOUT,DEFAULT_PING_TIMEOUT);
    }

    public String getDefaultIndexRefreshInterval() {
        return config.get(CONFIG_INDEX_REFRESH_INTERVAL,DEFAULT_INDEX_REFRESH_INTERVAL);
    }

    public boolean isDefaultIsMasterQuorumEnabled() {
        return config.get(CONFIG_IS_MASTER_QUORUM_ENABLED,DEFAULT_IS_MASTER_QUORUM_ENABLED);
    }

    public boolean isDefaultIsPingMulticastEnabled() {
        return config.get(CONFIG_IS_PING_MULTICAST_ENABLED,DEFAULT_IS_PING_MULTICAST_ENABLED);
    }

    public String getDefaultConfigBootclusterName() {
        return config.get(CONFIG_BOOTCLUSTER_NAME,DEFAULT_CONFIG_BOOTCLUSTER_NAME);
    }

    public String getDefaultEsDiscoveryType() {
        return config.get(CONFIG_ES_DISCOVERY_TYPE,DEFAULT_ES_DISCOVERY_TYPE);
    }

    public boolean isDefaultIsMultiDcEnabled() {
        return config.get(CONFIG_IS_MULTI_DC_ENABLED,DEFAULT_IS_MULTI_DC_ENABLED);
    }

    public boolean isDefaultIsAsgBasedDeploymentEnabled() {
        return config.get(CONFIG_IS_ASG_BASED_DEPLOYMENT_ENABLED,DEFAULT_IS_ASG_BASED_DEPLOYMENT_ENABLED);
    }

    public String getDefaultEsClusterRoutingAttributes() {
        return config.get(CONFIG_ES_CLUSTER_ROUTING_ATTRIBUTES,DEFAULT_ES_CLUSTER_ROUTING_ATTRIBUTES);
    }

    public String getDefaultEsProcessName() {
        return config.get(CONFIG_ES_PROCESS_NAME,DEFAULT_ES_PROCESS_NAME);
    }

    public boolean isDefaultIsShardAllocationPolicyEnabled() {
        return config.get(CONFIG_IS_SHARD_ALLOCATION_POLICY_ENABLED,DEFAULT_IS_SHARD_ALLOCATION_POLICY_ENABLED);
    }

    public String getDefaultEsShardAllocationAttribute() {
        return config.get(CONFIG_ES_SHARD_ALLOCATION_ATTRIBUTE,DEFAULT_ES_SHARD_ALLOCATION_ATTRIBUTE);
    }

    public String getDefaultConfigExtraParams() {
        return config.get(CONFIG_EXTRA_PARAMS,DEFAULT_CONFIG_EXTRA_PARAMS);
    }

    public boolean isDefaultIsDebugEnabled() {
        return config.get(CONFIG_IS_DEBUG_ENABLED,DEFAULT_IS_DEBUG_ENABLED);
    }

    public boolean isDefaultIsShardsPerNodeEnabled() {
        return config.get(CONFIG_IS_SHARDS_PER_NODE_ENABLED,DEFAULT_IS_SHARDS_PER_NODE_ENABLED);
    }

    public int getDefaultShardsPerNode() {
        return config.get(CONFIG_SHARDS_PER_NODE,DEFAULT_SHARDS_PER_NODE);
    }

    public boolean isDefaultIsIndexAutocreationEnabled() {
        return config.get(CONFIG_IS_INDEX_AUTOCREATION_ENABLED,DEFAULT_IS_INDEX_AUTOCREATION_ENABLED);
    }

    public int getDefaultAutocreateIndexTimeout() {
        return config.get(CONFIG_AUTOCREATE_INDEX_TIMEOUT,DEFAULT_AUTOCREATE_INDEX_TIMEOUT);
    }

    public int getDefaultAutocreateIndexInitialStartDelaySeconds() {
        return config.get(CONFIG_AUTOCREATE_INDEX_INITIAL_START_DELAY_SECONDS,DEFAULT_AUTOCREATE_INDEX_INITIAL_START_DELAY_SECONDS);
    }

    public int getDefaultAutocreateIndexPeriodicScheduledHour() {
        return config.get(CONFIG_AUTOCREATE_INDEX_PERIODIC_SCHEDULED_HOUR,DEFAULT_AUTOCREATE_INDEX_PERIODIC_SCHEDULED_HOUR);
    }

    public String getDefaultIndexMetadata() {
        return config.get(CONFIG_INDEX_METADATA,DEFAULT_INDEX_METADATA);
    }

    public String getDefaultBackupLocation() {
        return config.get(CONFIG_BACKUP_LOCATION,DEFAULT_BACKUP_LOCATION);
    }

    public int getDefaultBackupHour() {
        return config.get(CONFIG_BACKUP_HOUR,DEFAULT_BACKUP_HOUR);
    }

    public String getDefaultBackupCommaSeparatedIndices() {
        return config.get(CONFIG_BACKUP_COMMA_SEPARATED_INDICES,DEFAULT_BACKUP_COMMA_SEPARATED_INDICES);
    }

    public boolean isDefaultBackupPartialIndices() {
        return config.get(CONFIG_BACKUP_PARTIAL_INDICES,DEFAULT_BACKUP_PARTIAL_INDICES);
    }

    public boolean isDefaultBackupIncludeGlobalState() {
        return config.get(CONFIG_BACKUP_INCLUDE_GLOBAL_STATE,DEFAULT_BACKUP_INCLUDE_GLOBAL_STATE);
    }

    public boolean isDefaultBackupWaitForCompletion() {
        return config.get(CONFIG_BACKUP_WAIT_FOR_COMPLETION,DEFAULT_BACKUP_WAIT_FOR_COMPLETION);
    }

    public boolean isDefaultBackupIncludeIndexName() {
        return config.get(CONFIG_BACKUP_INCLUDE_INDEX_NAME,DEFAULT_BACKUP_INCLUDE_INDEX_NAME);
    }

    public boolean isDefaultIsRestoreEnabled() {
        return config.get(CONFIG_IS_RESTORE_ENABLED,DEFAULT_IS_RESTORE_ENABLED);
    }

    public String getDefaultRestoreRepositoryName() {
        return config.get(CONFIG_RESTORE_REPOSITORY_NAME,DEFAULT_RESTORE_REPOSITORY_NAME);
    }

    public String getDefaultRestoreRepositoryType() {
        return config.get(CONFIG_RESTORE_REPOSITORY_TYPE,DEFAULT_RESTORE_REPOSITORY_TYPE);
    }

    public String getDefaultRestoreSnapshotName() {
        return config.get(CONFIG_RESTORE_SNAPSHOT_NAME,DEFAULT_RESTORE_SNAPSHOT_NAME);
    }

    public String getDefaultRestoreCommaSeparatedIndices() {
        return config.get(CONFIG_RESTORE_COMMA_SEPARATED_INDICES,DEFAULT_RESTORE_COMMA_SEPARATED_INDICES);
    }

    public int getDefaultRestoreTaskInitialStartDelaySeconds() {
        return config.get(CONFIG_RESTORE_TASK_INITIAL_START_DELAY_SECONDS,DEFAULT_RESTORE_TASK_INITIAL_START_DELAY_SECONDS);
    }

    public String getDefaultRestoreSourceClusterName() {
        return config.get(CONFIG_RESTORE_SOURCE_CLUSTER_NAME,DEFAULT_RESTORE_SOURCE_CLUSTER_NAME);
    }

    public String getDefaultRestoreSourceRepoRegion() {
        return config.get(CONFIG_RESTORE_SOURCE_REPO_REGION,DEFAULT_RESTORE_SOURCE_REPO_REGION);
    }

    public String getDefaultRestoreLocation() {
        return config.get(CONFIG_RESTORE_LOCATION,DEFAULT_RESTORE_LOCATION);
    }

    public boolean isDefaultBackupIsSnapshotEnabled() {
        return config.get(CONFIG_BACKUP_IS_SNAPSHOT_ENABLED,DEFAULT_BACKUP_IS_SNAPSHOT_ENABLED);
    }

    public boolean isDefaultBackupIsHourlySnapshotEnabled() {
        return config.get(CONFIG_BACKUP_IS_HOURLY_SNAPSHOT_ENABLED,DEFAULT_BACKUP_IS_HOURLY_SNAPSHOT_ENABLED);
    }

    public long getDefaultBackupCronTimerSeconds() {
        return config.get(CONFIG_BACKUP_CRON_TIMER_SECONDS,DEFAULT_BACKUP_CRON_TIMER_SECONDS);
    }

    public boolean isDefaultAmITribeNode() {
        return config.get(CONFIG_AM_I_TRIBE_NODE,DEFAULT_AM_I_TRIBE_NODE);
    }

    public boolean isDefaultAmIWriteEnabledTribeNode() {
        return config.get(CONFIG_AM_I_WRITE_ENABLED_TRIBE_NODE,DEFAULT_AM_I_WRITE_ENABLED_TRIBE_NODE);
    }

    public boolean isDefaultAmIMetadataEnabledTribeNode() {
        return config.get(CONFIG_AM_I_METADATA_ENABLED_TRIBE_NODE,DEFAULT_AM_I_METADATA_ENABLED_TRIBE_NODE);
    }

    public String getDefaultTribeCommaSeparatedSourceClusters() {
        return config.get(CONFIG_TRIBE_COMMA_SEPARATED_SOURCE_CLUSTERS,DEFAULT_TRIBE_COMMA_SEPARATED_SOURCE_CLUSTERS);
    }

    public boolean isDefaultAmISourceClusterForTribeNode() {
        return config.get(CONFIG_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE,DEFAULT_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE);
    }

    public String getDefaultTribeCommaSeparatedTribeClusters() {
        return config.get(CONFIG_TRIBE_COMMA_SEPARATED_TRIBE_CLUSTERS,DEFAULT_TRIBE_COMMA_SEPARATED_TRIBE_CLUSTERS);
    }

    public boolean isDefaultIsNodemismatchWithDiscoveryEnabled() {
        return config.get(CONFIG_IS_NODEMISMATCH_WITH_DISCOVERY_ENABLED,DEFAULT_IS_NODEMISMATCH_WITH_DISCOVERY_ENABLED);
    }

    public int getDefaultDesiredNumNodesInCluster() {
        return config.get(CONFIG_DESIRED_NUM_NODES_IN_CLUSTER,DEFAULT_DESIRED_NUM_NODES_IN_CLUSTER);
    }

    public boolean isDefaultIsEurekaHealthCheckEnabled() {
        return config.get(CONFIG_IS_EUREKA_HEALTH_CHECK_ENABLED,DEFAULT_IS_EUREKA_HEALTH_CHECK_ENABLED);
    }

    public boolean isDefaultIsLocalModeEnabled() {
        return config.get(CONFIG_IS_LOCAL_MODE_ENABLED,DEFAULT_IS_LOCAL_MODE_ENABLED);
    }

    public String getDefaultCassandraKeyspaceName() {
        return config.get(CONFIG_CASSANDRA_KEYSPACE_NAME,DEFAULT_CASSANDRA_KEYSPACE_NAME);
    }

    public int getDefaultCassandraThriftPort() {
        return config.get(CONFIG_CASSANDRA_THRIFT_PORT,DEFAULT_CASSANDRA_THRIFT_PORT);
    }

    public boolean isDefaultIsEurekaHostSupplierEnabled() {
        return config.get(CONFIG_IS_EUREKA_HOST_SUPPLIER_ENABLED,DEFAULT_IS_EUREKA_HOST_SUPPLIER_ENABLED);
    }

    public String getDefaultCommaSeparatedCassandraHostnames() {
        return config.get(CONFIG_COMMA_SEPARATED_CASSANDRA_HOSTNAMES,DEFAULT_COMMA_SEPARATED_CASSANDRA_HOSTNAMES);
    }

    public boolean isDefaultIsSecurityGroupInMultiDc() {
        return config.get(CONFIG_IS_SECURITY_GROUP_IN_MULTI_DC,DEFAULT_IS_SECURITY_GROUP_IN_MULTI_DC);
    }

    public boolean isDefaultAmISourceClusterForTribeNodeInMultiDC() {
        return config.get(CONFIG_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE_IN_MULTI_DC,DEFAULT_AM_I_SOURCE_CLUSTER_FOR_TRIBE_NODE_IN_MULTI_DC);
    }

    public boolean getDefaultReportMetricsFromMasterOnly() {
        return config.get(CONFIG_REPORT_METRICS_FROM_MASTER_ONLY,DEFAULT_REPORT_METRICS_FROM_MASTER_ONLY);
    }
}
