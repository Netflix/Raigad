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
package com.netflix.raigad.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.scheduler.SimpleTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/*
 * This task checks if the Elasticsearch process is running.
 */
@Singleton
public class ElasticsearchProcessMonitor extends Task {

    public static final String JOBNAME = "ES_MONITOR_THREAD";
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchProcessMonitor.class);
    private static final AtomicBoolean isElasticsearchRunningNow = new AtomicBoolean(false);
    private static final AtomicBoolean wasElasticsearchStarted = new AtomicBoolean(false);
    private final HttpModule httpModule;

    @Inject
    protected ElasticsearchProcessMonitor(IConfiguration config, HttpModule httpModule) {
        super(config);
        this.httpModule = httpModule;
    }

    @Override
    public void execute() throws Exception {

        try {
            JSONObject response = getESResponse();
            Long status = analyzeResponse(response);
            updateStateHolders(status);
        } catch (Exception e) {
            logger.warn("Exception thrown while checking if Elasticsearch is running or not ", e);
            isElasticsearchRunningNow.set(false);
        }
    }

    protected static Long analyzeResponse(JSONObject response) {
        Long status = 0L;
        try {
            status = (Long)response.get("status");
        } catch (Exception ex) {
            logger.warn("Failed to reach localhost due to: " + ex.getLocalizedMessage());
            status = 500L;
        }
        return status;
    }

    protected JSONObject getESResponse() throws Exception {
        String URL = httpModule.localhostURL();
        String RESPONSE = SystemUtils.runHttpGetCommand(URL);
        return (JSONObject) new JSONParser().parse(RESPONSE);
    }

    protected void updateStateHolders(Long status) {
        if (status == 200L && !isElasticsearchRunning()) {
            isElasticsearchRunningNow.set(true);
            wasElasticsearchStarted.set(true);
        } else if (status != 200L && isElasticsearchRunning()) {
            isElasticsearchRunningNow.set(false);
        }
    }

    public static TaskTimer getTimer() {
        return new SimpleTimer(JOBNAME, 10L * 1000);
    }

    @Override
    public String getName() {
        return JOBNAME;
    }

    public static Boolean isElasticsearchRunning() {
        return isElasticsearchRunningNow.get();
    }

    public static Boolean getWasElasticsearchStarted() {
        return wasElasticsearchStarted.get();
    }

    //Added for testing only
    public static void setElasticsearchStarted() {
        isElasticsearchRunningNow.set(true);
    }
}
