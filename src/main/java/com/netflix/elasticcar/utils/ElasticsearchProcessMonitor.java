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
package com.netflix.elasticcar.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.IConfiguration;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;

/*
 * This task checks if the Elasticsearch process is running.
 */
@Singleton
public class ElasticsearchProcessMonitor extends Task{

	public static final String JOBNAME = "ES_MONITOR_THREAD";
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchProcessMonitor.class);
    private static final AtomicBoolean isElasticsearchStarted = new AtomicBoolean(false);

    @Inject
    protected ElasticsearchProcessMonitor(IConfiguration config) {
		super(config);
	}

	@Override
	public void execute() throws Exception {

        try
        {
        		//This returns pid for the Elasticsearch process
        		Process p = Runtime.getRuntime().exec("pgrep -f " + config.getElasticsearchProcessName());
        		BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = input.readLine();
        		if (line != null&& !isElasticsearchStarted())
        		{
        			isElasticsearchStarted.set(true);
        		}
        		else if(line  == null&& isElasticsearchStarted())
        		{
        			isElasticsearchStarted.set(false);
        		}
        }
        catch(Exception e)
        {
        	logger.warn("Exception thrown while checking if Elasticsearch is running or not ", e);
            isElasticsearchStarted.set(false);
        }
		
	}

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 10L * 1000);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public static Boolean isElasticsearchStarted()
    {
        return isElasticsearchStarted.get();
    }

    //Added for testing only
    public static void setElasticsearchStarted()
    {
		isElasticsearchStarted.set(true);
	}
}
