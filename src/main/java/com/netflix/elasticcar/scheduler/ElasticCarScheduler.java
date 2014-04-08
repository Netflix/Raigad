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
package com.netflix.elasticcar.scheduler;

import java.text.ParseException;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.utils.Sleeper;

/**
 * Scheduling class to schedule ElasticCar tasks. Uses Quartz scheduler
 */
@Singleton
public class ElasticCarScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticCarScheduler.class);
    private final Scheduler scheduler;
    private final GuiceJobFactory jobFactory;
    private final Sleeper sleeper;

    @Inject
    public ElasticCarScheduler(SchedulerFactory factory, GuiceJobFactory jobFactory, Sleeper sleeper)
    {
        try
        {
            this.scheduler = factory.getScheduler();
            this.scheduler.setJobFactory(jobFactory);
            this.jobFactory = jobFactory;
        }
        catch (SchedulerException e)
        {
            throw new RuntimeException(e);
        }
        this.sleeper = sleeper;
    }

    /**
     * Add a task to the scheduler
     */
    public void addTask(String name, Class<? extends Task> taskclass, TaskTimer timer) throws SchedulerException, ParseException
    {
        assert timer != null : "Cannot add scheduler task " + name + " as no timer is set";
        JobDetail job = new JobDetail(name, Scheduler.DEFAULT_GROUP, taskclass);
        scheduler.scheduleJob(job, timer.getTrigger());
    }

    /**
     * Add a delayed task to the scheduler
     */
    public void addTaskWithDelay(final String name, Class<? extends Task> taskclass, final TaskTimer timer, final int delayInSeconds) throws SchedulerException, ParseException
    {
        assert timer != null : "Cannot add scheduler task " + name + " as no timer is set";
        final JobDetail job = new JobDetail(name, Scheduler.DEFAULT_GROUP, taskclass);
        
        new Thread(new Runnable(){
            public void run()
            {
                try
                {
                        sleeper.sleepQuietly(delayInSeconds * 1000L);
                        scheduler.scheduleJob(job, timer.getTrigger());
                }
                catch (SchedulerException e)
                {
                    logger.warn("problem occurred while scheduling a job with name " + name, e);
                }
                catch (ParseException e)
                {
                    logger.warn("problem occurred while parsing a job with name " + name, e);
                }
            }
        }).start();
    }
    
    public void runTaskNow(Class<? extends Task> taskclass) throws Exception
    {
        jobFactory.guice.getInstance(taskclass).execute(null);
    }

    public void deleteTask(String name) throws SchedulerException, ParseException
    {
        scheduler.deleteJob(name, Scheduler.DEFAULT_GROUP);
    }

    public final Scheduler getScheduler()
    {
        return scheduler;
    }

    public void shutdown()
    {
        try
        {
            scheduler.shutdown();
        }
        catch (SchedulerException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void start()
    {
        try
        {
            scheduler.start();
        }
        catch (SchedulerException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
