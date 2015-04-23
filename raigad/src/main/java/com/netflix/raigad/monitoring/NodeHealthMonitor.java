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
package com.netflix.raigad.monitoring;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by alfasi on 4/22/15.
 */
@Singleton
public class NodeHealthMonitor extends Task {
    private static final Logger logger = LoggerFactory.getLogger(NodeHealthMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_NodeHealthMonitor";
    private final ElasticsearchNodeHealthReporter healthReporter;

    @Inject
    public NodeHealthMonitor(IConfiguration config) {
        super(config);
        healthReporter = new ElasticsearchNodeHealthReporter();
        Monitors.registerObject(healthReporter);
    }

    @Override
    public void execute() throws Exception {

        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.getWasElasticsearchStarted()) {
            String exceptionMsg = "Elasticsearch is not yet started, check back again later";
            logger.info(exceptionMsg);
            return;
        }

        HealthBean healthBean = new HealthBean();
        try {
            healthBean.esprocessdown = 0;
            if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
                logger.info("Elasticsearch process is up & running");
                healthBean.esprocessdown = 1;
            }
        } catch (Exception e) {
            resetHealthStats(healthBean);
            logger.warn("failed to check if Elasticsearch process is running", e);
        }

        healthReporter.healthBean.set(healthBean);
    }

    public class ElasticsearchNodeHealthReporter {
        private final AtomicReference<HealthBean> healthBean;

        public ElasticsearchNodeHealthReporter() {
            healthBean = new AtomicReference<HealthBean>(new HealthBean());
        }

        @Monitor(name = "es_isesprocessdown", type = DataSourceType.GAUGE)
        public int getIsEsProcessDown() {
            return healthBean.get().esprocessdown;
        }
    }

    private static class HealthBean {
        private int esprocessdown = -1;
    }

    @Override
    public String getName() {
        return METRIC_NAME;
    }

    public static TaskTimer getTimer(String name)
    {
        return new SimpleTimer(name, 60 * 1000);
    }

    private void resetHealthStats(HealthBean healthBean) {
        healthBean.esprocessdown = -1;
    }
}


