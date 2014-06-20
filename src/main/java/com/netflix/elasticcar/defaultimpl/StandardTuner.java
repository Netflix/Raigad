package com.netflix.elasticcar.defaultimpl;

import com.google.inject.Inject;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.IElasticsearchTuner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class StandardTuner implements IElasticsearchTuner
{
    private static final Logger logger = LoggerFactory.getLogger(StandardTuner.class);
    private static final String CL_BACKUP_PROPS_FILE = "/conf/commitlog_archiving.properties";
    protected final IConfiguration config;

    @Inject
    public StandardTuner(IConfiguration config)
    {
        this.config = config;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	public void writeAllProperties(String yamlLocation, String hostname) throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        map.put("cluster.name", config.getAppName());
        map.put("transport.tcp.port", config.getTransportTcpPort());
        map.put("http.port", config.getHttpPort());       
        map.put("path.data", config.getDataFileLocation());
        map.put("path.logs", config.getLogFileLocation());
        map.put("discovery.type", config.getElasticsearchDiscoveryType());
        map.put("discovery.zen.minimum_master_nodes",config.getMinimumMasterNodes());
        map.put("index.number_of_shards", config.getNumOfShards());
        map.put("index.number_of_replicas", config.getNumOfReplicas());
        map.put("index.refresh_interval", config.getIndexRefreshInterval());
//        if(config.isCustomShardAllocationPolicyEnabled())
//            map.put("cluster.routing.allocation.enable", config.getClusterShardAllocationAttribute());
        //NOTE: When using awareness attributes, shards will not be allocated to nodes 
        //that do not have values set for those attributes.
        //*** Important in dedicated master nodes deployment
        map.put("cluster.routing.allocation.awareness.attributes", config.getClusterRoutingAttributes());
		if (config.isMultiDC()) 
		{
			map.put("node.name", config.getRac() + "." + config.getInstanceId());
			map.put("node.rack_id", config.getDC());
			map.put("network.publish_host", config.getHostIP());
		}        
		else
        {
			map.put("node.name", config.getRac() + "." + config.getInstanceId());
			map.put("node.rack_id", config.getRac());
        }
		
		//TODO: Create New Tuner for ASG Based Deployment
		if(config.isAsgBasedDedicatedDeployment())
		{
			if(config.getASGName().toLowerCase().contains("master"))
			{
				map.put("node.master", true);
				map.put("node.data", false);
			}
			else if(config.getASGName().toLowerCase().contains("data"))
			{
				map.put("node.master", false);
				map.put("node.data", true);				
			}
			else if(config.getASGName().toLowerCase().contains("search"))
			{
				map.put("node.master", false);
				map.put("node.data", false);
			}
		}

        addExtraEsParams(map);

        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

    public void addExtraEsParams(Map map)
    {
        	String params = config.getExtraConfigParams();
        	if (params == null) {
            		logger.info("Updating yaml: no extra ES params");
            		return;
            }

            String[] pairs = params.split(",");
        	logger.info("Updating yaml: adding extra ES params");
        	for(int i=0; i<pairs.length; i++)
            {
                String[] pair = pairs[i].split("=");
        	    String escarKey = pair[0];
        		String esKey = pair[1];
        		String esVal = config.getEsKeyName(escarKey);
        		logger.info("Updating yaml: Elasticcarkey[" + escarKey + "], EsKey[" + esKey + "], Val[" + esVal + "]");
        		map.put(esKey, esVal);
        	}
        }
}
