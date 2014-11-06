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
package com.netflix.elasticcar.monitoring;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;
import com.netflix.elasticcar.utils.ESTransportClient;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class JvmStatsMonitor extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(JvmStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_JvmStatsMonitor";
    public static final String GC_YOUNG_TAG = "young";
    public static final String GC_OLD_TAG = "old";
    public static final String GC_SURVIVOR_TAG = "survivor";
    private final Elasticsearch_JvmStatsReporter jvmStatsReporter;

    @Inject
    public JvmStatsMonitor(IConfiguration config)
    {
        super(config);
        jvmStatsReporter = new Elasticsearch_JvmStatsReporter();
    		Monitors.registerObject(jvmStatsReporter);
    }

  	@Override
	public void execute() throws Exception {

		// If Elasticsearch is started then only start the monitoring
		if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
			String exceptionMsg = "Elasticsearch is not yet started, check back again later";
			logger.info(exceptionMsg);
			return;
		}

  		JvmStatsBean jvmStatsBean = new JvmStatsBean();
  		try
  		{
  			NodesStatsResponse ndsStatsResponse = ESTransportClient.getNodesStatsResponse(config);
  			JvmStats jvmStats = null;
  			NodeStats ndStat = null;
  			if (ndsStatsResponse.getNodes().length > 0) {
  				ndStat = ndsStatsResponse.getAt(0);
            }
			if (ndStat == null) {
				logger.info("NodeStats is null,hence returning (No JvmStats).");
				return;
			}
			jvmStats = ndStat.getJvm();
			if (jvmStats == null) {
				logger.info("JvmStats is null,hence returning (No JvmStats).");
				return;
			}

            //Heap
			jvmStatsBean.heapCommittedInBytes = jvmStats.getMem().getHeapCommitted().getMb();
			jvmStatsBean.heapMaxInBytes = jvmStats.getMem().getHeapMax().getMb();
			jvmStatsBean.heapUsedInBytes = jvmStats.getMem().getHeapUsed().getMb();
			jvmStatsBean.heapUsedPercent = jvmStats.getMem().getHeapUsedPrecent();
			jvmStatsBean.nonHeapCommittedInBytes = jvmStats.getMem().getNonHeapCommitted().getMb();
			jvmStatsBean.nonHeapUsedInBytes = jvmStats.getMem().getNonHeapUsed().getMb();
            Iterator<JvmStats.MemoryPool> memoryPoolIterator = jvmStats.getMem().iterator();
            while(memoryPoolIterator.hasNext())
            {
                JvmStats.MemoryPool mp = memoryPoolIterator.next();
                if(mp.getName().equalsIgnoreCase(GC_YOUNG_TAG))
                {
                    jvmStatsBean.youngMaxInBytes = mp.getMax().getBytes();
                    jvmStatsBean.youngUsedInBytes = mp.getUsed().getBytes();
                    jvmStatsBean.youngPeakUsedInBytes = mp.getPeakUsed().getBytes();
                    jvmStatsBean.youngPeakMaxInBytes = mp.getPeakMax().getBytes();
                }
                else if(mp.getName().equalsIgnoreCase(GC_SURVIVOR_TAG))
                {
                    jvmStatsBean.survivorMaxInBytes = mp.getMax().getBytes();
                    jvmStatsBean.survivorUsedInBytes = mp.getUsed().getBytes();
                    jvmStatsBean.survivorPeakUsedInBytes = mp.getPeakUsed().getBytes();
                    jvmStatsBean.survivorPeakMaxInBytes = mp.getPeakMax().getBytes();
                }
                else if(mp.getName().equalsIgnoreCase(GC_OLD_TAG))
                {
                    jvmStatsBean.oldMaxInBytes = mp.getMax().getBytes();
                    jvmStatsBean.oldUsedInBytes = mp.getUsed().getBytes();
                    jvmStatsBean.oldPeakUsedInBytes = mp.getPeakUsed().getBytes();
                    jvmStatsBean.oldPeakMaxInBytes = mp.getPeakMax().getBytes();
                }
            }
            //Threads
			jvmStatsBean.threadCount = jvmStats.getThreads().getCount();
			jvmStatsBean.threadPeakCount = jvmStats.getThreads().getPeakCount();
			jvmStatsBean.uptimeHours = jvmStats.getUptime().getHours();
            //GC
            for(JvmStats.GarbageCollector gc : jvmStats.getGc().collectors())
            {
               if(gc.getName().equalsIgnoreCase(GC_YOUNG_TAG))
               {
                   jvmStatsBean.youngCollectionCount = gc.getCollectionCount();
                   jvmStatsBean.youngCollectionTimeInMillis = gc.getCollectionTime().getMillis();
                   if(gc.getLastGc() != null)
                   {
                       jvmStatsBean.youngLastGcStartTime = gc.getLastGc().getStartTime();
                       jvmStatsBean.youngLastGcEndTime = gc.getLastGc().getEndTime();
                       jvmStatsBean.youngLastGcDuration = gc.getLastGc().getDuration().getMillis();
                       jvmStatsBean.youngLastGcMaxInBytes = gc.getLastGc().getMax().getBytes();
                       jvmStatsBean.youngLastGcBeforeUsedInBytes =  gc.getLastGc().getBeforeUsed().getBytes();
                       jvmStatsBean.youngLastGcAfterUsedInBytes = gc.getLastGc().getAfterUsed().getBytes();
                   }
               }else if(gc.getName().equalsIgnoreCase(GC_OLD_TAG))
               {
                   jvmStatsBean.oldCollectionCount = gc.getCollectionCount();
                   jvmStatsBean.oldCollectionTimeInMillis = gc.getCollectionTime().getMillis();
                   if(gc.getLastGc() != null)
                   {
                       jvmStatsBean.oldLastGcStartTime = gc.getLastGc().getStartTime();
                       jvmStatsBean.oldLastGcEndTime = gc.getLastGc().getEndTime();
                       jvmStatsBean.oldLastGcDuration = gc.getLastGc().getDuration().getMillis();
                       jvmStatsBean.oldLastGcMaxInBytes = gc.getLastGc().getMax().getBytes();
                       jvmStatsBean.oldLastGcBeforeUsedInBytes =  gc.getLastGc().getBeforeUsed().getBytes();
                       jvmStatsBean.oldLastGcAfterUsedInBytes = gc.getLastGc().getAfterUsed().getBytes();
                   }
               }
            }
            //Pools
  		}
  		catch(Exception e)
  		{
  			logger.warn("failed to load Jvm stats data", e);
  		}

  		jvmStatsReporter.jvmStatsBean.set(jvmStatsBean);
	}

    public class Elasticsearch_JvmStatsReporter
    {
        private final AtomicReference<JvmStatsBean> jvmStatsBean;

        public Elasticsearch_JvmStatsReporter()
        {
        		jvmStatsBean = new AtomicReference<JvmStatsBean>(new JvmStatsBean());
        }
        
        @Monitor(name ="heap_committed_in_bytes", type=DataSourceType.GAUGE)
        public long getHeapCommitedInBytes()
        {
            return jvmStatsBean.get().heapCommittedInBytes;
        }
        
        @Monitor(name ="heap_max_in_bytes", type=DataSourceType.GAUGE)
        public long getHeapMaxInBytes()
        {
            return jvmStatsBean.get().heapMaxInBytes;
        }
        @Monitor(name ="heap_used_in_bytes", type=DataSourceType.GAUGE)
        public long getHeapUsedInBytes()
        {
            return jvmStatsBean.get().heapUsedInBytes;
        }
        @Monitor(name ="non_heap_committed_in_bytes", type=DataSourceType.GAUGE)
        public long getNonHeapCommittedInBytes()
        {
            return jvmStatsBean.get().nonHeapCommittedInBytes;
        }
        @Monitor(name ="non_heap_used_in_bytes", type=DataSourceType.GAUGE)
        public long getNonHeapUsedInBytes()
        {
            return jvmStatsBean.get().nonHeapUsedInBytes;
        }
        @Monitor(name ="heap_used_percent", type=DataSourceType.GAUGE)
        public short getHeapUsedPercent()
        {
            return jvmStatsBean.get().heapUsedPercent;
        }
        @Monitor(name ="threads_count", type=DataSourceType.GAUGE)
        public long getThreadsCount()
        {
            return jvmStatsBean.get().threadCount;
        }
        @Monitor(name ="threads_peak_count", type=DataSourceType.GAUGE)
        public long getThreadsPeakCount()
        {
            return jvmStatsBean.get().threadPeakCount;
        }
        @Monitor(name ="uptime_hours", type=DataSourceType.GAUGE)
        public double getUptimeHours()
        {
            return jvmStatsBean.get().uptimeHours;
        }
        @Monitor(name ="young_collection_count", type=DataSourceType.GAUGE)
        public long getYoungCollectionCount()
        {
            return jvmStatsBean.get().youngCollectionCount;
        }
        @Monitor(name ="young_collection_time_in_millis", type=DataSourceType.GAUGE)
        public long getYoungCollectionTimeInMillis()
        {
            return jvmStatsBean.get().youngCollectionTimeInMillis;
        }
        @Monitor(name ="old_collection_count", type=DataSourceType.GAUGE)
        public long getOldCollectionCount()
        {
            return jvmStatsBean.get().oldCollectionCount;
        }
        @Monitor(name ="old_collection_time_in_millis", type=DataSourceType.GAUGE)
        public long getOldCollectionTimeInMillis()
        {
            return jvmStatsBean.get().oldCollectionTimeInMillis;
        }
        @Monitor(name ="young_used_in_bytes", type=DataSourceType.GAUGE)
        public long getYoungUsedInBytes()
        {
            return jvmStatsBean.get().youngUsedInBytes;
        }
        @Monitor(name ="young_max_in_bytes", type=DataSourceType.GAUGE)
        public long getYoungMaxInBytes()
        {
            return jvmStatsBean.get().youngMaxInBytes;
        }
        @Monitor(name ="young_peak_used_in_bytes", type=DataSourceType.GAUGE)
        public long getYoungPeakUsedInBytes()
        {
            return jvmStatsBean.get().youngPeakUsedInBytes;
        }
        @Monitor(name ="young_peak_max_in_bytes", type=DataSourceType.GAUGE)
        public long getYoungPeakMaxInBytes()
        {
            return jvmStatsBean.get().youngPeakMaxInBytes;
        }
        @Monitor(name ="survivor_used_in_bytes", type=DataSourceType.GAUGE)
        public long getSurvivorUsedInBytes()
        {
            return jvmStatsBean.get().survivorUsedInBytes;
        }
        @Monitor(name ="survivor_max_in_bytes", type=DataSourceType.GAUGE)
        public long getSurvivorMaxInBytes()
        {
            return jvmStatsBean.get().survivorMaxInBytes;
        }
        @Monitor(name ="survivor_peak_used_in_bytes", type=DataSourceType.GAUGE)
        public long getSurvivorPeakUsedInBytes()
        {
            return jvmStatsBean.get().survivorPeakUsedInBytes;
        }
        @Monitor(name ="survivor_peak_max_in_bytes", type=DataSourceType.GAUGE)
        public long getSurvivorPeakMaxInBytes()
        {
            return jvmStatsBean.get().survivorPeakMaxInBytes;
        }
        @Monitor(name ="old_used_in_bytes", type=DataSourceType.GAUGE)
        public long getOldUsedInBytes()
        {
            return jvmStatsBean.get().oldUsedInBytes;
        }
        @Monitor(name ="old_max_in_bytes", type=DataSourceType.GAUGE)
        public long getOldMaxInBytes()
        {
            return jvmStatsBean.get().oldMaxInBytes;
        }
        @Monitor(name ="old_peak_used_in_bytes", type=DataSourceType.GAUGE)
        public long getOldPeakUsedInBytes()
        {
            return jvmStatsBean.get().oldPeakUsedInBytes;
        }
        @Monitor(name ="old_peak_max_in_bytes", type=DataSourceType.GAUGE)
        public long getOldPeakMaxInBytes()
        {
            return jvmStatsBean.get().oldPeakMaxInBytes;
        }
        @Monitor(name ="young_last_gc_start_time", type=DataSourceType.GAUGE)
        public long getYoungLastGcStartTime()
        {
            return jvmStatsBean.get().youngLastGcStartTime;
        }
        @Monitor(name ="young_last_gc_end_time", type=DataSourceType.GAUGE)
        public long getYoungLastGcEndTime()
        {
            return jvmStatsBean.get().youngLastGcEndTime;
        }
        @Monitor(name ="young_last_gc_max_in_bytes", type=DataSourceType.GAUGE)
        public long getYoungLastGcMaxInBytes()
        {
            return jvmStatsBean.get().youngLastGcMaxInBytes;
        }
        @Monitor(name ="young_last_gc_before_used_in_bytes", type=DataSourceType.GAUGE)
        public long getYoungLastGcBeforeUsedInBytes()
        {
            return jvmStatsBean.get().youngLastGcBeforeUsedInBytes;
        }
        @Monitor(name ="young_last_gc_after_used_in_bytes", type=DataSourceType.GAUGE)
        public long getYoungLastGcAfterUsedInBytes()
        {
            return jvmStatsBean.get().youngLastGcAfterUsedInBytes;
        }
        @Monitor(name ="young_last_gc_duration", type=DataSourceType.GAUGE)
        public long getYoungLastGcDuration()
        {
            return jvmStatsBean.get().youngLastGcDuration;
        }
        @Monitor(name ="old_last_gc_start_time", type=DataSourceType.GAUGE)
        public long getOldLastGcStartTime()
        {
            return jvmStatsBean.get().oldLastGcStartTime;
        }
        @Monitor(name ="old_last_gc_end_time", type=DataSourceType.GAUGE)
        public long getOldLastGcEndTime()
        {
            return jvmStatsBean.get().oldLastGcEndTime;
        }
        @Monitor(name ="old_last_gc_max_in_bytes", type=DataSourceType.GAUGE)
        public long getOldLastGcMaxInBytes()
        {
            return jvmStatsBean.get().oldLastGcMaxInBytes;
        }
        @Monitor(name ="old_last_gc_before_used_in_bytes", type=DataSourceType.GAUGE)
        public long getOldLastGcBeforeUsedInBytes()
        {
            return jvmStatsBean.get().oldLastGcBeforeUsedInBytes;
        }
        @Monitor(name ="old_last_gc_after_used_in_bytes", type=DataSourceType.GAUGE)
        public long getOldLastGcAfterUsedInBytes()
        {
            return jvmStatsBean.get().oldLastGcAfterUsedInBytes;
        }
        @Monitor(name ="old_last_gc_duration", type=DataSourceType.GAUGE)
        public long getOldLastGcDuration()
        {
            return jvmStatsBean.get().oldLastGcDuration;
        }
    }

    private static class JvmStatsBean
    {
        private long heapCommittedInBytes = -1;
        private long heapMaxInBytes = -1;
        private long heapUsedInBytes = -1;
        private long nonHeapCommittedInBytes = -1;
        private long nonHeapUsedInBytes = -1;
        private short heapUsedPercent = -1;
        private int threadCount = -1;
        private int threadPeakCount = -1;
        private long uptimeHours = -1;
        private long youngCollectionCount = -1;
        private long youngCollectionTimeInMillis = -1;
        private long oldCollectionCount = -1;
        private long oldCollectionTimeInMillis = -1;
        private long youngUsedInBytes = -1;
        private long youngMaxInBytes = -1;
        private long youngPeakUsedInBytes = -1;
        private long youngPeakMaxInBytes = -1;
        private long survivorUsedInBytes = -1;
        private long survivorMaxInBytes = -1;
        private long survivorPeakUsedInBytes = -1;
        private long survivorPeakMaxInBytes = -1;
        private long oldUsedInBytes = -1;
        private long oldMaxInBytes = -1;
        private long oldPeakUsedInBytes = -1;
        private long oldPeakMaxInBytes = -1;
        private long youngLastGcStartTime = -1;
        private long youngLastGcEndTime = -1;
        private long youngLastGcMaxInBytes= -1;
        private long youngLastGcBeforeUsedInBytes = -1;
        private long youngLastGcAfterUsedInBytes = -1;
        private long youngLastGcDuration = -1;
        private long oldLastGcStartTime = -1;
        private long oldLastGcEndTime = -1;
        private long oldLastGcMaxInBytes= -1;
        private long oldLastGcBeforeUsedInBytes = -1;
        private long oldLastGcAfterUsedInBytes = -1;
        private long oldLastGcDuration = -1;
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
