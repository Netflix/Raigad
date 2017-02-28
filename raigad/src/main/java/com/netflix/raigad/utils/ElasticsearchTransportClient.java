/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.raigad.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class ElasticsearchTransportClient {
    protected static final Logger logger = LoggerFactory.getLogger(ElasticsearchTransportClient.class);

    protected static AtomicReference<ElasticsearchTransportClient> esTransportClient = new AtomicReference<>(null);

    protected NodesStatsRequestBuilder nodeStatsRequestBuilder;
    protected final TransportClient client;

    /**
     * Hostname and port to talk to will be same server for now optionally we might want the IP to poll.
     * NOTE: This class shouldn't be a singleton and this shouldn't be cached.
     * This will work only if Elasticsearch runs.
     */
    public ElasticsearchTransportClient(InetAddress host, int port, String clusterName, String nodeName) throws IOException, InterruptedException {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();

        client = TransportClient.builder().settings(settings).build();
        client.addTransportAddress(new InetSocketTransportAddress(host, port));

        nodeStatsRequestBuilder = client.admin().cluster().prepareNodesStats(nodeName).all();
    }

    @Inject
    public ElasticsearchTransportClient(IConfiguration config) throws IOException, InterruptedException {
        this(InetAddress.getLocalHost(), config.getTransportTcpPort(), config.getAppName(), config.getEsNodeName());
    }

    /**
     * Try to create if it is null.
     *
     * @throws IOException
     */
    public static ElasticsearchTransportClient instance(IConfiguration config) throws ElasticsearchTransportClientConnectionException {
        if (esTransportClient.get() == null) {
            esTransportClient.set(connect(config));
        }

        return esTransportClient.get();
    }

    public static NodesStatsResponse getNodesStatsResponse(IConfiguration config) {
        try {
            return ElasticsearchTransportClient.instance(config).nodeStatsRequestBuilder.execute().actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }

    protected static synchronized ElasticsearchTransportClient connect(final IConfiguration config) throws ElasticsearchTransportClientConnectionException {
        ElasticsearchTransportClient transportClient;

        // If Elasticsearch is started then only start the monitoring
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            String exceptionMessage = "Elasticsearch is not yet started, check back again later";
            logger.info("Elasticsearch is not yet started, check back again later");
            throw new ElasticsearchTransportClientConnectionException(exceptionMessage);
        }

        try {
            transportClient = new BoundedExponentialRetryCallable<ElasticsearchTransportClient>() {
                @Override
                public ElasticsearchTransportClient retriableCall() throws Exception {
                    ElasticsearchTransportClient transportClientLocal = new ElasticsearchTransportClient(
                            InetAddress.getLocalHost(),
                            config.getTransportTcpPort(),
                            config.getAppName(),
                            config.getEsNodeName());

                    return transportClientLocal;
                }
            }.call();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ElasticsearchTransportClientConnectionException(e.getMessage());
        }

        return transportClient;
    }

    protected JSONObject createJson(String primaryEndpoint, String dataCenter, String rack, String status,
                                  String state, String load, String owns, String token) throws JSONException {
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

    public Client getTransportClient() {
        return client;
    }
}
