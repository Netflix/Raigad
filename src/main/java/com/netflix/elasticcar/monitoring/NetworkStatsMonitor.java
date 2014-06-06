package com.netflix.elasticcar.monitoring;

import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.monitor.network.NetworkStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.IConfiguration;
import com.netflix.elasticcar.scheduler.SimpleTimer;
import com.netflix.elasticcar.scheduler.Task;
import com.netflix.elasticcar.scheduler.TaskTimer;
import com.netflix.elasticcar.utils.ESTransportClient;
import com.netflix.elasticcar.utils.ElasticsearchProcessMonitor;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

@Singleton
public class NetworkStatsMonitor extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(NetworkStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_FsMonitor";
    private final NetworkStatsReporter networkStatsReporter;
    
    @Inject
    public NetworkStatsMonitor(IConfiguration config)
    {
        super(config);
        logger.info("***Inside constructor NetworkStatsMonitor");
        networkStatsReporter = new NetworkStatsReporter();
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
				return;
			}
			networkStats = ndStat.getNetwork();
			if (networkStats == null) {
				logger.info("NetworkStats is null,hence returning (No NetworkStats).");
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
	    	  	logger.info("activeOpens = "+networkStatsBean.activeOpens);
	    	  	logger.info("passiveOpens = "+networkStatsBean.passiveOpens);
	    	  	logger.info("attemptFails = "+networkStatsBean.attemptFails);
	    	  	logger.info("estabResets = "+networkStatsBean.estabResets);
	    	  	logger.info("currEstab = "+networkStatsBean.currEstab);
	    	  	logger.info("inSegs = "+networkStatsBean.inSegs);
	    	  	logger.info("outSegs = "+networkStatsBean.outSegs);
	    	  	logger.info("retransSegs = "+networkStatsBean.retransSegs);
	    	  	logger.info("inErrs = "+networkStatsBean.inErrs);
	    	  	logger.info("outRsts = "+networkStatsBean.outRsts);
  		}
  		catch(Exception e)
  		{
  			logger.warn("failed to load Network stats data", e);
  		}

  		networkStatsReporter.networkStatsBean.set(networkStatsBean);
	}
  	
    public class NetworkStatsReporter
    {
        private final AtomicReference<NetworkStatsBean> networkStatsBean;

        public NetworkStatsReporter()
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

}
