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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.configuration.IConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
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
    private final TransportClient client;

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
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(host,port));
        
        ndStatsRequestBuilder = client.admin().cluster().prepareNodesStats("_local").all();
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
   		if (esTransportClient.get() == null)
        		esTransportClient.set(connect(config));
        
        return esTransportClient.get();
    }

    public static NodesStatsResponse getNodesStatsResponse(IConfiguration config)
    {
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

    public TransportClient getTransportClient(){
        return client;
    }

}
