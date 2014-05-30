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

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.IConfiguration;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;

/**
 * Class to get data out of Elasticsearch
 */
@Singleton
public class ESTransportClient
{
    private static final Logger logger = LoggerFactory.getLogger(ESTransportClient.class);
    private static AtomicReference<ESTransportClient> esTransportClient = new AtomicReference<ESTransportClient>(null);
    private NodesStatsRequestBuilder ndStatsRequestBuilder;

    /**
     * Hostname and Port to talk to will be same server for now optionally we
     * might want the ip to poll.
     * 
     * NOTE: This class shouldn't be a singleton and this shouldn't be cached.
     * 
     * This will work only if Elasticsearch runs.
     */
    public ESTransportClient(String host, int port, String clusterName) throws IOException, InterruptedException
    {
    		logger.info("***Inside ESTransportClient ctr ...");
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        TransportClient client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(host,port));
        
        ndStatsRequestBuilder = client.admin().cluster().prepareNodesStats("_local").all();
        logger.info("***Done constructing NodesStatsRequestBuilder...");
    }

    @Inject
    public ESTransportClient(IConfiguration config) throws IOException, InterruptedException
    {
        this("localhost", config.getTransportTcpPort(), config.getAppName());
    }

    /**
     * try to create if it is null.
     * @throws IOException 
     */
    public static ESTransportClient instance(IConfiguration config) throws ESTransportClientConnectionException
    {
   		logger.info("***Inside ESTransportClient instance ...");
   		if (esTransportClient.get() == null)
        		esTransportClient.set(connect(config));
        
        return esTransportClient.get();
    }

    public static NodesStatsResponse getNodesStatsResponse(IConfiguration config)
    {
   		logger.info("***Inside ESTransportClient getNodesStatsResponse ...");
   		try
        {
             return ESTransportClient.instance(config).ndStatsRequestBuilder.execute().actionGet();
        }
        catch (Exception e)
        {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public static synchronized ESTransportClient connect(final IConfiguration config) throws ESTransportClientConnectionException
    {
    		ESTransportClient ESTransportClient = null;
    		
		// If Elasticsearch is started then only start the monitoring
		if (!ElasticsearchProcessMonitor.isElasticsearchStarted()) {
			String exceptionMsg = "Elasticsearch is not yet started, check back again later";
			//TODO: Change logger to debug
			logger.info(exceptionMsg);
			throw new ESTransportClientConnectionException(exceptionMsg);
		}        		
    		
    		try {
    				ESTransportClient = new BoundedExponentialRetryCallable<ESTransportClient>()
						{
							@Override
							public ESTransportClient retriableCall() throws Exception
							{
								ESTransportClient esTransportClientLocal = new ESTransportClient("localhost", config.getTransportTcpPort(),config.getAppName());
						   		logger.info("***Returning ESTransportClient from connect ...");
						   		return esTransportClientLocal;
							}
						}.call();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new ESTransportClientConnectionException(e.getMessage());
			}
    		return ESTransportClient;
    }


    private JSONObject createJson(String primaryEndpoint, String dataCenter, String rack, String status, String state, String load, String owns, String token) throws JSONException
    {
        JSONObject object = new JSONObject();
        object.put("endpoint", primaryEndpoint);
        object.put("dc", dataCenter);
        object.put("rack", rack);
        object.put("status", status);
        object.put("state", state);
        object.put("load", load);
        object.put("owns", owns);
        object.put("token", token.toString());
        return object;
    }



//  /**
//   * This method will test if you can connect and query something before handing over the connection,
//   * This is required for our retry logic.
//   * @return
//   */
//  private static boolean testConnection()
//  {
//      // connecting first time hence return false.
//      if (esTransportClient.get() == null)
//          return false;
//      
//      try
//      {
//      		esTransportClient.get().isInitialized();
//      }
//      catch (Throwable ex)
//      {
//          SystemUtils.closeQuietly(tool);
//          return false;
//      }
//      return true;
//  }

//    @Override
//    public void close() throws IOException
//    {
//        synchronized (ESTransportClient.class)
//        {
//            tool = null;
//            super.close();
//        }
//    }
}
