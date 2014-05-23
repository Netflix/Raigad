package com.netflix.elasticcar.defaultimpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.google.inject.Inject;
import com.netflix.elasticcar.IConfiguration;
import com.netflix.elasticcar.utils.IElasticsearchTuner;

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
        map.put("cluster.routing.allocation.awareness.attributes", config.getClusterRoutingAttributes());
		if (config.isMultiDC()) 
		{
			map.put("node.name", config.getDC() + "." + config.getInstanceId());
			map.put("node.rack_id", config.getDC());
			map.put("network.publish_host", config.getHostIP());
		}        
		else
        {
			map.put("node.name", config.getDC() + "." + config.getInstanceId());
			map.put("node.rack_id", config.getRac());
        }
        
//        List<?> seedp = (List) map.get("seed_provider");
//        Map<String, String> m = (Map<String, String>) seedp.get(0);
//        m.put("class_name", seedProvider);

        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }

   

//    String derivePartitioner(String fromYaml, String fromConfig)
//    {
//        if(fromYaml == null || fromYaml.isEmpty())
//            return fromConfig;
//        //this check is to prevent against overwriting an existing yaml file that has
//        // a partitioner not RandomPartitioner or (as of cass 1.2) Murmur3Partitioner.
//        //basically we don't want to hose existing deployments by changing the partitioner unexpectedly on them
//        final String lowerCase = fromYaml.toLowerCase();
//        if(lowerCase.contains("randomparti") || lowerCase.contains("murmur"))
//            return fromConfig;
//        return fromYaml;
//    }

   

//    public void updateAutoBootstrap(String yamlFile, boolean autobootstrap) throws IOException
//    {
//        DumperOptions options = new DumperOptions();
//        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
//        Yaml yaml = new Yaml(options);
//        @SuppressWarnings("rawtypes")
//        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
//        //Dont bootstrap in restore mode
//        map.put("auto_bootstrap", autobootstrap);
//        logger.info("Updating yaml" + yaml.dump(map));
//        yaml.dump(map, new FileWriter(yamlFile));
//    }
}
