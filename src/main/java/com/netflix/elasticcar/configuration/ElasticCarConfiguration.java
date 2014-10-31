/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.elasticcar.configuration;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.aws.ICredential;
import com.netflix.elasticcar.utils.RetryableCallable;
import com.netflix.elasticcar.utils.SystemUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Singleton
public class ElasticCarConfiguration implements IConfiguration
{
    public static final String ESCAR_PRE = "escar";

    private static final String CONFIG_ES_HOME_DIR = ESCAR_PRE + ".elasticsearch.home";
    private static final String CONFIG_ES_START_SCRIPT = ESCAR_PRE + ".elasticsearch.startscript";
    private static final String CONFIG_ES_STOP_SCRIPT = ESCAR_PRE + ".elasticsearch.stopscript";
    private static final String CONFIG_CLUSTER_NAME = ESCAR_PRE + ".clustername";
    private static final String CONFIG_DATA_LOCATION = ESCAR_PRE + ".data.location";
    private static final String CONFIG_SEED_PROVIDER_NAME = ESCAR_PRE + ".seed.provider";
    private static final String CONFIG_ES_LISTERN_PORT_NAME = ESCAR_PRE + ".es.port";
    private static final String CONFIG_AVAILABILITY_ZONES = ESCAR_PRE + ".zones.available";
    private static final String CONFIG_ES_PEER_PORT_NAME = ESCAR_PRE + ".es.peer.port";
    private static final String CONFIG_STORAGE_LISTERN_PORT_NAME = ESCAR_PRE + ".storage.port";
    private static final String CONFIG_BOOTCLUSTER_NAME = ESCAR_PRE + ".bootcluster";
    private static final String CONFIG_ES_PROCESS_NAME = ESCAR_PRE + ".elasticsearch.process";
    private static final String CONFIG_YAML_LOCATION = ESCAR_PRE + ".yamlLocation";
    private static final String CONFIG_PARTITIONER = ESCAR_PRE + ".partitioner";
    private static final String CONFIG_EXTRA_PARAMS = ESCAR_PRE + ".extra.params";

    // Amazon specific
    private static final String CONFIG_ASG_NAME = ESCAR_PRE + ".az.asgname";
    private static final String CONFIG_REGION_NAME = ESCAR_PRE + ".az.region";
    private static final String CONFIG_ACL_GROUP_NAME = ESCAR_PRE + ".acl.groupname";
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
    private final String DEFAULT_CLUSTER_NAME = "elasticsearch_demo1";
    private final String DEFAULT_DATA_LOCATION = "/mnt/data/es";

    private final String DEFAULT_SEED_PROVIDER = "com.netflix.escar.elasticsearch.NFSeedProvider";
    private final String DEFAULT_ES_HOME_DIR = "/apps/elasticsearch";
    private final String DEFAULT_ES_START_SCRIPT = "/etc/init.d/elasticsearch start";
    private final String DEFAULT_ES_STOP_SCRIPT = "/etc/init.d/elasticsearch stop";
    private List<String> DEFAULT_AVAILABILITY_ZONES = ImmutableList.of();
    private final String DEFAULT_ES_PROCESS_NAME = "elasticsearch";
    private final int DEFAULT_ES_LISTENER_PORT = 7102;
    private final int DEFAULT_ES_PEER_PORT = 7101;
    private final int DEFAULT_STORAGE_PORT = 11211;
   
    private final IConfigSource config; 
    private static final Logger logger = LoggerFactory.getLogger(ElasticCarConfiguration.class);
    private final ICredential provider;

    @Inject
    public ElasticCarConfiguration(ICredential provider, IConfigSource config)
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
        //SystemUtils.createDirs(getDataFileLocation());
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
    public String getElasticsearchStartupScript()
    {
        return config.get(CONFIG_ES_START_SCRIPT, DEFAULT_ES_START_SCRIPT);
    }

    @Override
    public String getElasticsearchStopScript()
    {
        return config.get(CONFIG_ES_STOP_SCRIPT, DEFAULT_ES_STOP_SCRIPT);
    }

    @Override
    public String getElasticsearchHome()
    {
        return config.get(CONFIG_ES_HOME_DIR, DEFAULT_ES_HOME_DIR);
    }

    @Override
    public String getDataFileLocation()
    {
        return config.get(CONFIG_DATA_LOCATION, DEFAULT_DATA_LOCATION);
    }


    @Override
    public int getElasticsearchListenerPort()
    {
        return config.get(CONFIG_ES_LISTERN_PORT_NAME, DEFAULT_ES_LISTENER_PORT);
    }


    @Override
    public int getTransportTcpPort()
    {
        return config.get(CONFIG_ES_PEER_PORT_NAME, DEFAULT_ES_PEER_PORT);
    }

    @Override
    public int getHttpPort()
    {
        return config.get(CONFIG_STORAGE_LISTERN_PORT_NAME, DEFAULT_STORAGE_PORT);
    }


    @Override
    public String getAppName()
    {
        return config.get(CONFIG_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    }

    @Override
    public String getRac()
    {
        return RAC;
    }

    @Override
    public List<String> getRacs()
    {
        return config.getList(CONFIG_AVAILABILITY_ZONES, DEFAULT_AVAILABILITY_ZONES);
    }

    @Override
    public String getHostname()
    {
        return PUBLIC_HOSTNAME;
    }

    @Override
    public String getInstanceName()
    {
        return INSTANCE_ID;
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
        return config.get(CONFIG_ASG_NAME, "elasticsearch_demo1");
    }

    @Override
    public String getACLGroupName()
    {
    	return config.get(CONFIG_ACL_GROUP_NAME, this.getAppName());
    }

   
    @Override
    public String getHostIP()
    {
        return PUBLIC_IP;
    }

    @Override
    public String getHostLocalIP() {
        return LOCAL_IP;
    }

    @Override
    public String getBootClusterName()
    {
        return config.get(CONFIG_BOOTCLUSTER_NAME, "cass_abc");
    }

    @Override
    public String getSeedProviderName()
    {
        return config.get(CONFIG_SEED_PROVIDER_NAME, DEFAULT_SEED_PROVIDER);
    }

 
    public String getPartitioner()
    {
        return config.get(CONFIG_PARTITIONER);
    }   

	@Override
	public String getElasticsearchProcessName() {
        return config.get(CONFIG_ES_PROCESS_NAME, DEFAULT_ES_PROCESS_NAME);
	}


    public String getYamlLocation()
    {
        return config.get(CONFIG_YAML_LOCATION, getElasticsearchHome() + "/conf/elasticsearch.yaml");
    }

    @Override
    public String getBackupLocation() {
        return null;
    }

    @Override
	public boolean isMultiDC() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean doesElasticsearchStartManually() {
		return false;
	}

	@Override
	public int getNumOfShards() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumOfReplicas() {
		// TODO Auto-generated method stub
		return 0;
	}

    @Override
    public int getTotalShardsPerNode() {
        return 0;
    }

    @Override
	public String getRefreshInterval() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isMasterQuorumEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getMinimumMasterNodes() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getPingTimeout() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isPingMulticastEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getFdPingInterval() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFdPingTimeout() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLogFileLocation() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getElasticsearchDiscoveryType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInstanceId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getIndexRefreshInterval() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClusterRoutingAttributes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAsgBasedDedicatedDeployment() {
		// TODO Auto-generated method stub
		return false;
	}

    @Override
    public String getClusterShardAllocationAttribute() {
        return null;
    }

    @Override
    public String getExtraConfigParams() {
        return config.get(CONFIG_EXTRA_PARAMS);
    }

    @Override
    public String getEsKeyName(String escarKey) {
        return config.get(escarKey);
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
        return null;
    }

    @Override
    public int getAutoCreateIndexTimeout() {
        return 0;
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
        return null;
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
        return false;
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
        return null;
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
        return 9160;
    }

    @Override
    public boolean isCustomShardAllocationPolicyEnabled() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


}
