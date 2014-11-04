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
import org.elasticsearch.transport.TransportStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class TransportStatsMonitor extends Task
{
	private static final Logger logger = LoggerFactory.getLogger(TransportStatsMonitor.class);
    public static final String METRIC_NAME = "Elasticsearch_TransportMonitor";
    private final Elasticsearch_TransportStatsReporter transportStatsReporter;
    
    @Inject
    public TransportStatsMonitor(IConfiguration config)
    {
        super(config);
        transportStatsReporter = new Elasticsearch_TransportStatsReporter();
    		Monitors.registerObject(transportStatsReporter);
    }

  	@Override
	public void execute() throws Exception {

		// If Elasticsearch is started then only start the monitoring
		if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
			String exceptionMsg = "Elasticsearch is not yet started, check back again later";
			logger.info(exceptionMsg);
			return;
		}        		
	
  		TransportStatsBean transportStatsBean = new TransportStatsBean();
  		try
  		{
  			NodesStatsResponse ndsStatsResponse = ESTransportClient.getNodesStatsResponse(config);
  			TransportStats transportStats = null;
  			NodeStats ndStat = null;
  			if (ndsStatsResponse.getNodes().length > 0) {
  				ndStat = ndsStatsResponse.getAt(0);
            }
			if (ndStat == null) {
				logger.info("NodeStats is null,hence returning (No TransportStats).");
				return;
			}
			transportStats = ndStat.getTransport();
			if (transportStats == null) {
				logger.info("TransportStats is null,hence returning (No TransportStats).");
				return;
			}
	
			transportStatsBean.serverOpen = transportStats.getServerOpen();
			transportStatsBean.rxCount = transportStats.getRxCount();
			transportStatsBean.rxSize = transportStats.getRxSize().getBytes();
			transportStatsBean.rxSizeDelta = transportStats.getRxSize().getBytes() - transportStatsBean.rxSize;
			transportStatsBean.txCount = transportStats.getTxCount();
			transportStatsBean.txSize = transportStats.getTxSize().getBytes();
			transportStatsBean.txSizeDelta = transportStats.getTxSize().getBytes() - transportStatsBean.txSize;  	
  		}
  		catch(Exception e)
  		{
  			logger.warn("failed to load Transport stats data", e);
  		}

  		transportStatsReporter.transportStatsBean.set(transportStatsBean);
	}
  	
    public class Elasticsearch_TransportStatsReporter
    {
        private final AtomicReference<TransportStatsBean> transportStatsBean;

        public Elasticsearch_TransportStatsReporter()
        {
        		transportStatsBean = new AtomicReference<TransportStatsBean>(new TransportStatsBean());
        }
        
        @Monitor(name ="server_open", type=DataSourceType.GAUGE)
        public long getServerOpen()
        {
            return transportStatsBean.get().serverOpen;
        }
        
        @Monitor(name ="rx_count", type=DataSourceType.GAUGE)
        public long getRxCount()
        {
            return transportStatsBean.get().rxCount;
        }
        @Monitor(name ="rx_size", type=DataSourceType.GAUGE)
        public long getRxSize()
        {
            return transportStatsBean.get().rxSize;
        }
        @Monitor(name ="rx_size_delta", type=DataSourceType.GAUGE)
        public long getRxSizeDelta()
        {
            return transportStatsBean.get().rxSizeDelta;
        }
        @Monitor(name ="tx_count", type=DataSourceType.GAUGE)
        public long getTxCount()
        {
            return transportStatsBean.get().txCount;
        }
        @Monitor(name ="tx_size", type=DataSourceType.GAUGE)
        public long getTxSize()
        {
            return transportStatsBean.get().txSize;
        }
        @Monitor(name ="tx_size_delta", type=DataSourceType.GAUGE)
        public long getTxSizeDelta()
        {
            return transportStatsBean.get().txSizeDelta;
        }
    }
    
    private static class TransportStatsBean
    {
    	  private long serverOpen;
    	  private long rxCount=0;
    	  private long rxSize=0;
    	  private long rxSizeDelta=0;
    	  private long txCount=0;
    	  private long txSize=0;
    	  private long txSizeDelta=0;    	  
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
