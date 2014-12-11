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
import com.netflix.raigad.utils.ESTransportClient;
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.monitor.process.ProcessStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class ProcessStatsMonitor extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(ProcessStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_ProcessStatsMonitor";
    private final Elasticsearch_ProcessStatsReporter processStatsReporter;

    @Inject
    public ProcessStatsMonitor(IConfiguration config)
    {
        super(config);
        processStatsReporter = new Elasticsearch_ProcessStatsReporter();
    	Monitors.registerObject(processStatsReporter);
    }

  	@Override
	public void execute() throws Exception {

		// If Elasticsearch is started then only start the monitoring
		if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
			String exceptionMsg = "Elasticsearch is not yet started, check back again later";
			logger.info(exceptionMsg);
			return;
		}

  		ProcessStatsBean processStatsBean = new ProcessStatsBean();
  		try
  		{
  			NodesStatsResponse ndsStatsResponse = ESTransportClient.getNodesStatsResponse(config);
  			ProcessStats processStats = null;
  			NodeStats ndStat = null;
  			if (ndsStatsResponse.getNodes().length > 0) {
  				ndStat = ndsStatsResponse.getAt(0);
            }
			if (ndStat == null) {
				logger.info("NodeStats is null,hence returning (No ProcessStats).");
                resetProcessStats(processStatsBean);
				return;
			}
			processStats = ndStat.getProcess();
			if (processStats == null) {
				logger.info("ProcessStats is null,hence returning (No ProcessStats).");
                resetProcessStats(processStatsBean);
				return;
			}

            //Mem
			processStatsBean.residentInBytes = processStats.getMem().getResident().getBytes();
			processStatsBean.shareInBytes = processStats.getMem().getShare().getBytes();
			processStatsBean.totalVirtualInBytes = processStats.getMem().getTotalVirtual().getBytes();
            //CPU
			processStatsBean.cpuPercent = processStats.getCpu().getPercent();
			processStatsBean.sysInMillis = processStats.getCpu().getSys().getMillis();
			processStatsBean.userInMillis = processStats.getCpu().getUser().getMillis();
			processStatsBean.totalInMillis = processStats.getCpu().getTotal().getMillis();
            //Open File Descriptors
			processStatsBean.openFileDescriptors = processStats.getOpenFileDescriptors();
            //Timestamp
			processStatsBean.cpuTimestamp = processStats.getTimestamp();
  		}
  		catch(Exception e)
  		{
            resetProcessStats(processStatsBean);
  			logger.warn("failed to load Process stats data", e);
  		}

  		processStatsReporter.processStatsBean.set(processStatsBean);
	}

    public class Elasticsearch_ProcessStatsReporter
    {
        private final AtomicReference<ProcessStatsBean> processStatsBean;

        public Elasticsearch_ProcessStatsReporter()
        {
        		processStatsBean = new AtomicReference<ProcessStatsBean>(new ProcessStatsBean());
        }
        
        @Monitor(name ="resident_in_bytes", type=DataSourceType.GAUGE)
        public long getResidentInBytes()
        {
            return processStatsBean.get().residentInBytes;
        }
        
        @Monitor(name ="share_in_bytes", type=DataSourceType.GAUGE)
        public long getShareInBytes()
        {
            return processStatsBean.get().shareInBytes;
        }
        @Monitor(name ="total_virtual_in_bytes", type=DataSourceType.GAUGE)
        public long getTotalVirtualInBytes()
        {
            return processStatsBean.get().totalVirtualInBytes;
        }
        @Monitor(name ="cpu_percent", type=DataSourceType.GAUGE)
        public short getCpuPercent()
        {
            return processStatsBean.get().cpuPercent;
        }
        @Monitor(name ="sys_in_millis", type=DataSourceType.GAUGE)
        public long getSysInMillis()
        {
            return processStatsBean.get().sysInMillis;
        }
        @Monitor(name ="user_in_millis", type=DataSourceType.GAUGE)
        public long getUserInMillis()
        {
            return processStatsBean.get().userInMillis;
        }
        @Monitor(name ="total_in_millis", type=DataSourceType.GAUGE)
        public long getTotalInMillis()
        {
            return processStatsBean.get().totalInMillis;
        }
        @Monitor(name ="open_file_descriptors", type=DataSourceType.GAUGE)
        public double getOpenFileDescriptors()
        {
            return processStatsBean.get().openFileDescriptors;
        }
        @Monitor(name ="cpu_timestamp", type=DataSourceType.GAUGE)
        public long getCpuTimestamp()
        {
            return processStatsBean.get().cpuTimestamp;
        }
    }
    
    private static class ProcessStatsBean
    {
    	  private long residentInBytes = -1;
    	  private long shareInBytes = -1;
    	  private long totalVirtualInBytes = -1;
    	  private short cpuPercent = -1;
    	  private long sysInMillis = -1;
    	  private long userInMillis = -1;
    	  private long totalInMillis = -1;
    	  private long openFileDescriptors = -1;
    	  private long cpuTimestamp = -1;
    }

	public static TaskTimer getTimer(String name)
	{
		return new SimpleTimer(name, 60 * 1000);
	}

	@Override
	public String getName()
	{
		return METRIC_NAME;
	}

    private void resetProcessStats(ProcessStatsBean processStatsBean){
        processStatsBean.residentInBytes = -1;
        processStatsBean.shareInBytes = -1;
        processStatsBean.totalVirtualInBytes = -1;
        processStatsBean.cpuPercent = -1;
        processStatsBean.sysInMillis = -1;
        processStatsBean.userInMillis = -1;
        processStatsBean.totalInMillis = -1;
        processStatsBean.openFileDescriptors = -1;
        processStatsBean.cpuTimestamp = -1;
    }
}
