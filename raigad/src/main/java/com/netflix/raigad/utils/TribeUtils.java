/**
 * Copyright 2016 Netflix, Inc.
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

@Singleton
public class TribeUtils {
    private static final Logger logger = LoggerFactory.getLogger(TribeUtils.class);
    private final IConfiguration config;

    @Inject
    public TribeUtils(IConfiguration config) {
        this.config = config;
    }

    public String getTribeClusterNameFromId(String tribeId) throws FileNotFoundException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml yaml = new Yaml(options);
        File yamlFile = new File(config.getYamlLocation());
        Map map = (Map) yaml.load(new FileInputStream(yamlFile));

        String sourceClusterName = (String) map.get("tribe." + tribeId + ".cluster.name");

        logger.info("Source cluster associated with tribe ID {} is {}", tribeId, sourceClusterName);

        return sourceClusterName;
    }
}
