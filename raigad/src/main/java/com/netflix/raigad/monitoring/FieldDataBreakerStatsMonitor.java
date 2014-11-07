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
import org.elasticsearch.indices.fielddata.breaker.FieldDataBreakerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class FieldDataBreakerStatsMonitor extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(FieldDataBreakerStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_FieldDataBreakerStatsMonitor";
    private final Elasticsearch_FieldDataBreakerStatsReporter fieldDataBreakerStatsReporter;

    @Inject
    public FieldDataBreakerStatsMonitor(IConfiguration config)
    {
        super(config);
        fieldDataBreakerStatsReporter = new Elasticsearch_FieldDataBreakerStatsReporter();
    	Monitors.registerObject(fieldDataBreakerStatsReporter);
    }

  	@Override
	public void execute() throws Exception {

		// If Elasticsearch is started then only start the monitoring
		if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
			String exceptionMsg = "Elasticsearch is not yet started, check back again later";
			logger.info(exceptionMsg);
			return;
		}

  		FieldDataBreakerStatsBean fieldDataBreakerStatsBean = new FieldDataBreakerStatsBean();
  		try
  		{
  			NodesStatsResponse ndsStatsResponse = ESTransportClient.getNodesStatsResponse(config);
  			FieldDataBreakerStats fieldDataBreakerStats = null;
  			NodeStats ndStat = null;
  			if (ndsStatsResponse.getNodes().length > 0) {
  				ndStat = ndsStatsResponse.getAt(0);
            }
			if (ndStat == null) {
				logger.info("NodeStats is null,hence returning (No FieldDataBreakerStats).");
				return;
			}
			fieldDataBreakerStats = ndStat.getBreaker();
			if (fieldDataBreakerStats == null) {
				logger.info("FieldDataBreakerStats is null,hence returning (No FieldDataBreakerStats).");
				return;
			}

			fieldDataBreakerStatsBean.estimatedSizeInBytes = fieldDataBreakerStats.getEstimated();
			fieldDataBreakerStatsBean.maximumSizeInBytes = fieldDataBreakerStats.getMaximum();
			fieldDataBreakerStatsBean.overhead = fieldDataBreakerStats.getOverhead();
  		}
  		catch(Exception e)
  		{
  			logger.warn("failed to load FieldDataBreaker stats data", e);
  		}

  		fieldDataBreakerStatsReporter.fieldDataBreakerStatsBean.set(fieldDataBreakerStatsBean);
	}

    public class Elasticsearch_FieldDataBreakerStatsReporter
    {
        private final AtomicReference<FieldDataBreakerStatsBean> fieldDataBreakerStatsBean;

        public Elasticsearch_FieldDataBreakerStatsReporter()
        {
        		fieldDataBreakerStatsBean = new AtomicReference<FieldDataBreakerStatsBean>(new FieldDataBreakerStatsBean());
        }
        
        @Monitor(name ="estimated_size_in_bytes", type=DataSourceType.GAUGE)
        public long getEstimatedSizeInBytes()
        {
            return fieldDataBreakerStatsBean.get().estimatedSizeInBytes;
        }
        
        @Monitor(name ="maximum_size_in_bytes", type=DataSourceType.GAUGE)
        public long getMaximumSizeInBytes()
        {
            return fieldDataBreakerStatsBean.get().maximumSizeInBytes;
        }
        @Monitor(name ="overhead", type=DataSourceType.GAUGE)
        public double getOverhead()
        {
            return fieldDataBreakerStatsBean.get().overhead;
        }
    }
    
    private static class FieldDataBreakerStatsBean
    {
    	  private long estimatedSizeInBytes = -1;
    	  private long maximumSizeInBytes = -1;
    	  private double overhead = -1;
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

}
