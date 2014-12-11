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
import org.elasticsearch.monitor.network.NetworkStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class NetworkStatsMonitor extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(NetworkStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_FsMonitor";
    private final Elasticsearch_NetworkStatsReporter networkStatsReporter;
    
    @Inject
    public NetworkStatsMonitor(IConfiguration config)
    {
        super(config);
        networkStatsReporter = new Elasticsearch_NetworkStatsReporter();
    		Monitors.registerObject(networkStatsReporter);
    }

  	@Override
	public void execute() throws Exception {

		// If Elasticsearch is started then only start the monitoring
		if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
			String exceptionMsg = "Elasticsearch is not yet started, check back again later";
			logger.info(exceptionMsg);
			return;
		}        		
	
  		NetworkStatsBean networkStatsBean = new NetworkStatsBean();
  		try
  		{
  			NodesStatsResponse ndsStatsResponse = ESTransportClient.getNodesStatsResponse(config);
  			NetworkStats networkStats = null;
  			NodeStats ndStat = null;
  			if (ndsStatsResponse.getNodes().length > 0) {
  				ndStat = ndsStatsResponse.getAt(0);
            }
			if (ndStat == null) {
				logger.info("NodeStats is null,hence returning (No NetworkStats).");
                resetNetworkStats(networkStatsBean);
				return;
			}
			networkStats = ndStat.getNetwork();
			if (networkStats == null) {
				logger.info("NetworkStats is null,hence returning (No NetworkStats).");
                resetNetworkStats(networkStatsBean);
				return;
			}
	
			networkStatsBean.activeOpens = networkStats.getTcp().getActiveOpens();
			networkStatsBean.passiveOpens = networkStats.getTcp().getPassiveOpens();
			networkStatsBean.attemptFails = networkStats.getTcp().getAttemptFails();
			networkStatsBean.estabResets = networkStats.getTcp().getEstabResets();
			networkStatsBean.currEstab = networkStats.getTcp().getCurrEstab();
			networkStatsBean.inSegs = networkStats.getTcp().getInSegs();
			networkStatsBean.outSegs = networkStats.getTcp().getOutSegs();  	
			networkStatsBean.retransSegs = networkStats.getTcp().getRetransSegs();
			networkStatsBean.inErrs = networkStats.getTcp().getInErrs();
			networkStatsBean.outRsts = networkStats.getTcp().getOutRsts();
  		}
  		catch(Exception e)
  		{
            resetNetworkStats(networkStatsBean);
  			logger.warn("failed to load Network stats data", e);
  		}

  		networkStatsReporter.networkStatsBean.set(networkStatsBean);
	}
  	
    public class Elasticsearch_NetworkStatsReporter
    {
        private final AtomicReference<NetworkStatsBean> networkStatsBean;

        public Elasticsearch_NetworkStatsReporter()
        {
        		networkStatsBean = new AtomicReference<NetworkStatsBean>(new NetworkStatsBean());
        }
        
        @Monitor(name ="active_opens", type=DataSourceType.GAUGE)
        public long getActiveOpens()
        {
            return networkStatsBean.get().activeOpens;
        }
        
        @Monitor(name ="passive_opens", type=DataSourceType.GAUGE)
        public long getPassiveOpens()
        {
            return networkStatsBean.get().passiveOpens;
        }
        @Monitor(name ="attempt_fails", type=DataSourceType.GAUGE)
        public long getAttemptFails()
        {
            return networkStatsBean.get().attemptFails;
        }
        @Monitor(name ="estab_resets", type=DataSourceType.GAUGE)
        public long geEstabResets()
        {
            return networkStatsBean.get().estabResets;
        }
        @Monitor(name ="curr_estab", type=DataSourceType.GAUGE)
        public long getCurrEstab()
        {
            return networkStatsBean.get().currEstab;
        }
        @Monitor(name ="in_segs", type=DataSourceType.GAUGE)
        public long getInSegs()
        {
            return networkStatsBean.get().inSegs;
        }
        @Monitor(name ="out_segs", type=DataSourceType.GAUGE)
        public long getOutSegs()
        {
            return networkStatsBean.get().outSegs;
        }
        @Monitor(name ="retrans_segs", type=DataSourceType.GAUGE)
        public double getRetransSegs()
        {
            return networkStatsBean.get().retransSegs;
        }
        @Monitor(name ="in_errs", type=DataSourceType.GAUGE)
        public double getInErrs()
        {
            return networkStatsBean.get().inErrs;
        }
        @Monitor(name ="outRsts", type=DataSourceType.GAUGE)
        public double getOutRsts()
        {
            return networkStatsBean.get().outRsts;
        }
    }

    private static class NetworkStatsBean
    {
        private long activeOpens=0;
        private long passiveOpens=0;
        private long attemptFails=0;
        private long estabResets=0;
        private long currEstab=0;
        private long inSegs=0;
        private long outSegs=0;
        private long retransSegs=0;
        private long inErrs=0;
        private long outRsts=0;

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

    private void resetNetworkStats(NetworkStatsBean networkStatsBean){
        networkStatsBean.activeOpens=0;
        networkStatsBean.passiveOpens=0;
        networkStatsBean.attemptFails=0;
        networkStatsBean.estabResets=0;
        networkStatsBean.currEstab=0;
        networkStatsBean.inSegs=0;
        networkStatsBean.outSegs=0;
        networkStatsBean.retransSegs=0;
        networkStatsBean.inErrs=0;
        networkStatsBean.outRsts=0;
    }
}
