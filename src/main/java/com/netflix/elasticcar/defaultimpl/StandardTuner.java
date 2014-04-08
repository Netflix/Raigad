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
import com.netflix.elasticcar.utils.ElasticsearchTuner;

public class StandardTuner implements ElasticsearchTuner
{
    private static final Logger logger = LoggerFactory.getLogger(StandardTuner.class);
    private static final String CL_BACKUP_PROPS_FILE = "/conf/commitlog_archiving.properties";
    protected final IConfiguration config;

    @Inject
    public StandardTuner(IConfiguration config)
    {
        this.config = config;
    }

    public void writeAllProperties(String yamlLocation, String hostname, String seedProvider) throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(yamlLocation);
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        map.put("cluster_name", config.getAppName());
        map.put("storage_port", config.getStoragePort());
        map.put("es_listen", config.getElasticsearchPeerListenerPort());       
        map.put("listen_address", hostname);
        map.put("rpc_address", hostname);

        List<?> seedp = (List) map.get("seed_provider");
        Map<String, String> m = (Map<String, String>) seedp.get(0);
        m.put("class_name", seedProvider);

        //force to 1 until vnodes are properly supported
	    map.put("num_tokens", 1);

        logger.info(yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));

    }

   

    String derivePartitioner(String fromYaml, String fromConfig)
    {
        if(fromYaml == null || fromYaml.isEmpty())
            return fromConfig;
        //this check is to prevent against overwriting an existing yaml file that has
        // a partitioner not RandomPartitioner or (as of cass 1.2) Murmur3Partitioner.
        //basically we don't want to hose existing deployments by changing the partitioner unexpectedly on them
        final String lowerCase = fromYaml.toLowerCase();
        if(lowerCase.contains("randomparti") || lowerCase.contains("murmur"))
            return fromConfig;
        return fromYaml;
    }

   

    public void updateAutoBootstrap(String yamlFile, boolean autobootstrap) throws IOException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        @SuppressWarnings("rawtypes")
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        //Dont bootstrap in restore mode
        map.put("auto_bootstrap", autobootstrap);
        logger.info("Updating yaml" + yaml.dump(map));
        yaml.dump(map, new FileWriter(yamlFile));
    }
}
