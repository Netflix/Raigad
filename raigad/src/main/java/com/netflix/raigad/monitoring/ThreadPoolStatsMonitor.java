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
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class ThreadPoolStatsMonitor extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(ThreadPoolStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_ThreadPoolMonitor";
    private final Elasticsearch_ThreadPoolStatsReporter tpStatsReporter;
    
    @Inject
    public ThreadPoolStatsMonitor(IConfiguration config)
    {
        super(config);
        tpStatsReporter = new Elasticsearch_ThreadPoolStatsReporter();
    		Monitors.registerObject(tpStatsReporter);
    }

  	@Override
	public void execute() throws Exception {

		// If Elasticsearch is started then only start the monitoring
		if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
			String exceptionMsg = "Elasticsearch is not yet started, check back again later";
			logger.info(exceptionMsg);
			return;
		}        		
	
  		ThreadPoolStatsBean tpStatsBean = new ThreadPoolStatsBean();
  		try
  		{
  			NodesStatsResponse ndsStatsResponse = ESTransportClient.getNodesStatsResponse(config);
  			ThreadPoolStats tpstats = null;
  			NodeStats ndStat = null;
  			if (ndsStatsResponse.getNodes().length > 0) {
  				ndStat = ndsStatsResponse.getAt(0);
            }
			if (ndStat == null) {
				logger.info("NodeStats is null,hence returning (No ThreadPoolStats).");
                resetThreadPoolStats(tpStatsBean);
				return;
			}
  			tpstats = ndStat.getThreadPool();
			if (tpstats == null) {
				logger.info("ThreadPoolStats is null,hence returning (No ThreadPoolStats).");
                resetThreadPoolStats(tpStatsBean);
				return;
			}
  		    Iterator<ThreadPoolStats.Stats> iter = tpstats.iterator();
  		    while( iter.hasNext() ) {
  		      ThreadPoolStats.Stats stat = iter.next();
  		      if( stat.getName().equals("index") ) {
  		    	  	tpStatsBean.indexThreads = stat.getThreads();
  		    	  	tpStatsBean.indexQueue = stat.getQueue();
  		    	  	tpStatsBean.indexActive = stat.getActive();
  		    	  	tpStatsBean.indexRejected = stat.getRejected();
  		    	  	tpStatsBean.indexLargest = stat.getLargest();
  		    	  	tpStatsBean.indexCompleted = stat.getCompleted();
  		      }
  		      else if( stat.getName().equals("get") ) {
  		    	  	tpStatsBean.getThreads = stat.getThreads();
  		    	  	tpStatsBean.getQueue = stat.getQueue();
  		    	  	tpStatsBean.getActive = stat.getActive();
  		    	  	tpStatsBean.getRejected = stat.getRejected();
  		    	  	tpStatsBean.getLargest = stat.getLargest();
  		    	  	tpStatsBean.getCompleted = stat.getCompleted();
  		      }
  		      else if( stat.getName().equals("search") ) {
  		    	  	tpStatsBean.searchThreads = stat.getThreads();
  		    	  	tpStatsBean.searchQueue = stat.getQueue();
  		    	  	tpStatsBean.searchActive = stat.getActive();
  		    	  	tpStatsBean.searchRejected = stat.getRejected();
  		    	  	tpStatsBean.searchLargest = stat.getLargest();
  		    	  	tpStatsBean.searchCompleted = stat.getCompleted();
  		      }
  		      else if( stat.getName().equals("bulk") ) {
  		    	  	tpStatsBean.bulkThreads = stat.getThreads();
  		    	  	tpStatsBean.bulkQueue = stat.getQueue();
  		    	  	tpStatsBean.bulkActive = stat.getActive();
  		    	  	tpStatsBean.bulkRejected = stat.getRejected();
  		    	  	tpStatsBean.bulkLargest = stat.getLargest();
  		    	  	tpStatsBean.bulkCompleted = stat.getCompleted();
  		      }
  		    }
  		}
  		catch(Exception e)
  		{
            resetThreadPoolStats(tpStatsBean);
  			logger.warn("failed to load Thread Pool stats data", e);
  		}
  		tpStatsReporter.threadPoolBean.set(tpStatsBean);
	}
  	
    public class Elasticsearch_ThreadPoolStatsReporter
    {
        private final AtomicReference<ThreadPoolStatsBean> threadPoolBean;

        public Elasticsearch_ThreadPoolStatsReporter()
        {
        		threadPoolBean = new AtomicReference<ThreadPoolStatsBean>(new ThreadPoolStatsBean());
        }
        
        @Monitor(name="IndexThreads", type=DataSourceType.GAUGE)
        public long getIndexThreads()
        {
            return threadPoolBean.get().indexThreads;
        }
        
        @Monitor(name="IndexQueue", type=DataSourceType.GAUGE)
        public long getIndexQueue()
        {
            return threadPoolBean.get().indexQueue;
        }
        @Monitor(name="indexActive", type=DataSourceType.GAUGE)
        public long getIndexActive()
        {
            return threadPoolBean.get().indexActive;
        }
        @Monitor(name="indexRejected", type=DataSourceType.COUNTER)
        public long getIndexRejected()
        {
            return threadPoolBean.get().indexRejected;
        }
        @Monitor(name="indexLargest", type=DataSourceType.GAUGE)
        public long getIndexLargest()
        {
            return threadPoolBean.get().indexLargest;
        }
        @Monitor(name="indexCompleted", type=DataSourceType.COUNTER)
        public long getIndexCompleted()
        {
            return threadPoolBean.get().indexCompleted;
        }

        @Monitor(name="getThreads", type=DataSourceType.GAUGE)
        public long getGetThreads()
        {
            return threadPoolBean.get().getThreads;
        }
        @Monitor(name="getQueue", type=DataSourceType.GAUGE)
        public long getGetQueue()
        {
            return threadPoolBean.get().getQueue;
        }
        @Monitor(name="getActive", type=DataSourceType.GAUGE)
        public long getGetActive()
        {
            return threadPoolBean.get().getActive;
        }
        @Monitor(name="getRejected", type=DataSourceType.COUNTER)
        public long getGetRejected()
        {
            return threadPoolBean.get().getRejected;
        }
        @Monitor(name="getLargest", type=DataSourceType.GAUGE)
        public long getGetLargest()
        {
            return threadPoolBean.get().getLargest;
        }
        @Monitor(name="getCompleted", type=DataSourceType.COUNTER)
        public long getGetCompleted()
        {
            return threadPoolBean.get().getCompleted;
        }

        @Monitor(name="searchThreads", type=DataSourceType.GAUGE)
        public long getSearchThreads()
        {
            return threadPoolBean.get().searchThreads;
        }
        @Monitor(name="searchQueue", type=DataSourceType.GAUGE)
        public long getSearchQueue()
        {
            return threadPoolBean.get().searchQueue;
        }
        @Monitor(name="searchActive", type=DataSourceType.GAUGE)
        public long getSearchActive()
        {
            return threadPoolBean.get().searchActive;
        }
        @Monitor(name="searchRejected", type=DataSourceType.COUNTER)
        public long getSearchRejected()
        {
            return threadPoolBean.get().searchRejected;
        }
        @Monitor(name="searchLargest", type=DataSourceType.GAUGE)
        public long getSearchLargest()
        {
            return threadPoolBean.get().searchLargest;
        }
        @Monitor(name="searchCompleted", type=DataSourceType.COUNTER)
        public long getSearchCompleted()
        {
            return threadPoolBean.get().searchCompleted;
        }

        @Monitor(name="bulkThreads", type=DataSourceType.GAUGE)
        public long getBulkThreads()
        {
            return threadPoolBean.get().bulkThreads;
        }
        @Monitor(name="bulkQueue", type=DataSourceType.GAUGE)
        public long getBulkQueue()
        {
            return threadPoolBean.get().bulkQueue;
        }
        @Monitor(name="bulkActive", type=DataSourceType.GAUGE)
        public long getBulkActive()
        {
            return threadPoolBean.get().bulkActive;
        }
        @Monitor(name="bulkRejected", type=DataSourceType.COUNTER)
        public long getBulkRejected()
        {
            return threadPoolBean.get().bulkRejected;
        }
        @Monitor(name="bulkLargest", type=DataSourceType.GAUGE)
        public long getBulkLargest()
        {
            return threadPoolBean.get().bulkLargest;
        }
        @Monitor(name="bulkCompleted", type=DataSourceType.COUNTER)
        public long getBulkCompleted()
        {
            return threadPoolBean.get().bulkCompleted;
        }
    }
    
    private static class ThreadPoolStatsBean
    {
        private long indexThreads = -1;
        private long indexQueue = -1;
        private long indexActive = -1;
        private long indexRejected = -1;
        private long indexLargest = -1;
        private long indexCompleted = -1;

        private long getThreads = -1;
        private long getQueue = -1;
        private long getActive = -1;
        private long getRejected = -1;
        private long getLargest = -1;
        private long getCompleted = -1;

        private long searchThreads = -1;
        private long searchQueue = -1;
        private long searchActive = -1;
        private long searchRejected = -1;
        private long searchLargest = -1;
        private long searchCompleted = -1;

        private long bulkThreads = -1;
        private long bulkQueue = -1;
        private long bulkActive = -1;
        private long bulkRejected = -1;
        private long bulkLargest = -1;
        private long bulkCompleted = -1;
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

    private void resetThreadPoolStats(ThreadPoolStatsBean threadPoolStatsBean){
        threadPoolStatsBean.indexThreads = -1;
        threadPoolStatsBean.indexQueue = -1;
        threadPoolStatsBean.indexActive = -1;
        threadPoolStatsBean.indexRejected = -1;
        threadPoolStatsBean.indexLargest = -1;
        threadPoolStatsBean.indexCompleted = -1;

        threadPoolStatsBean.getThreads = -1;
        threadPoolStatsBean.getQueue = -1;
        threadPoolStatsBean.getActive = -1;
        threadPoolStatsBean.getRejected = -1;
        threadPoolStatsBean.getLargest = -1;
        threadPoolStatsBean.getCompleted = -1;

        threadPoolStatsBean.searchThreads = -1;
        threadPoolStatsBean.searchQueue = -1;
        threadPoolStatsBean.searchActive = -1;
        threadPoolStatsBean.searchRejected = -1;
        threadPoolStatsBean.searchLargest = -1;
        threadPoolStatsBean.searchCompleted = -1;

        threadPoolStatsBean.bulkThreads = -1;
        threadPoolStatsBean.bulkQueue = -1;
        threadPoolStatsBean.bulkActive = -1;
        threadPoolStatsBean.bulkRejected = -1;
        threadPoolStatsBean.bulkLargest = -1;
        threadPoolStatsBean.bulkCompleted = -1;
    }
}
