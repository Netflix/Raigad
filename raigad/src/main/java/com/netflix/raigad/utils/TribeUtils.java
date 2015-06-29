package com.netflix.raigad.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Created by sloke on 6/26/15.
 */
@Singleton
public class TribeUtils
{
    private static final Logger logger = LoggerFactory.getLogger(TribeUtils.class);
    private final IConfiguration config;

    @Inject
    public TribeUtils(IConfiguration config)
    {
        this.config = config;
    }

    public String getTribeClusterNameFromId(String tribeId) throws FileNotFoundException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(config.getYamlLocation());
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));
        String sourceClusterName = (String) map.get("tribe."+tribeId+".cluster.name");
        logger.info("Source Cluster associated with tribeId = {} is {}",tribeId,sourceClusterName);
        return sourceClusterName;
    }
}
