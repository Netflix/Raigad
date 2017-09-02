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
package com.netflix.raigad.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * This task checks if the Elasticsearch process is running.
 */
@Singleton
public class ElasticsearchProcessMonitor extends Task {

    public static final String JOB_NAME = "ES_MONITOR_THREAD";

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchProcessMonitor.class);

    static final AtomicBoolean isElasticsearchRunningNow = new AtomicBoolean(false);
    static final AtomicBoolean wasElasticsearchStarted = new AtomicBoolean(false);

    @Inject
    protected ElasticsearchProcessMonitor(IConfiguration config) {
        super(config);
    }

    @Override
    public void execute() throws Exception {
        checkElasticsearchProcess(config.getElasticsearchProcessName());
    }

    static void checkElasticsearchProcess(String elasticsearchProcessName) throws Exception {
        Process pgrepProcess = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;

        try {
            // This returns PID for the Elasticsearch process
            pgrepProcess = Runtime.getRuntime().exec("pgrep -f " + elasticsearchProcessName);
            inputStreamReader = new InputStreamReader(pgrepProcess.getInputStream());
            bufferedReader = new BufferedReader(inputStreamReader);

            String line = StringUtils.trim(bufferedReader.readLine());

            if (StringUtils.isNotEmpty(line) && !isElasticsearchRunning()) {
                isElasticsearchRunningNow.set(true);
                if (!wasElasticsearchStarted.get()) {
                    wasElasticsearchStarted.set(true);
                }
            } else if (StringUtils.isEmpty(line) && isElasticsearchRunning()) {
                isElasticsearchRunningNow.set(false);
            }
        } catch (Exception e) {
            logger.warn("Exception checking if process is running", e);
            isElasticsearchRunningNow.set(false);
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (inputStreamReader != null) {
                inputStreamReader.close();
            }
            if (pgrepProcess != null) {
                pgrepProcess.destroyForcibly();
            }
        }
    }

    public static TaskTimer getTimer() {
        return new SimpleTimer(JOB_NAME, 10L * 1000);
    }

    @Override
    public String getName() {
        return JOB_NAME;
    }

    public static Boolean isElasticsearchRunning() {
        return isElasticsearchRunningNow.get();
    }

    public static Boolean getWasElasticsearchStarted() {
        return wasElasticsearchStarted.get();
    }
}
