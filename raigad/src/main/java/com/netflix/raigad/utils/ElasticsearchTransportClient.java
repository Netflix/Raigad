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
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class ElasticsearchTransportClient {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTransportClient.class);

    private static AtomicReference<ElasticsearchTransportClient> elasticsearchTransportClientAtomicReference = new AtomicReference<>(null);

    private NodesStatsRequestBuilder nodeStatsRequestBuilder;
    private final TransportClient client;

    /**
     * Hostname and port to talk to will be same server for now optionally we might want the IP to poll.
     * NOTE: This class shouldn't be a singleton and this shouldn't be cached.
     * This will work only if Elasticsearch runs.
     */
    private ElasticsearchTransportClient(InetAddress host, IConfiguration configuration)
            throws IOException, InterruptedException {

        logger.info("Initializing client connection to {}", host.toString());

        Map<String, String> transportClientSettings = new HashMap<>();
        transportClientSettings.put("cluster.name", configuration.getAppName());
        transportClientSettings.put("client.transport.ping_timeout", "30s");
        transportClientSettings.put("client.transport.nodes_sampler_interval", "30s");

        client = new PreBuiltTransportClient(Settings.builder().put(transportClientSettings).build());
        client.addTransportAddress(new InetSocketTransportAddress(host, configuration.getTransportTcpPort()));

        nodeStatsRequestBuilder = client.admin().cluster().prepareNodesStats(configuration.getEsNodeName()).all();
    }

    @Inject
    public ElasticsearchTransportClient(IConfiguration configuration) throws IOException, InterruptedException {
        this(InetAddress.getLocalHost(), configuration);
    }

    /**
     * Try to create if it is null
     *
     * @throws ElasticsearchTransportClientConnectionException
     */
    public static ElasticsearchTransportClient instance(IConfiguration configuration) throws ElasticsearchTransportClientConnectionException {
        if (elasticsearchTransportClientAtomicReference.get() == null) {
            elasticsearchTransportClientAtomicReference.set(connect(configuration));
        }

        return elasticsearchTransportClientAtomicReference.get();
    }

    public static NodesStatsResponse getNodesStatsResponse(IConfiguration config) {
        try {
            return ElasticsearchTransportClient.instance(config).getNodeStatsRequestBuilder().execute().actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }

    private static synchronized ElasticsearchTransportClient connect(final IConfiguration configuration) throws ElasticsearchTransportClientConnectionException {
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
                    return new ElasticsearchTransportClient(InetAddress.getLocalHost(), configuration);
                }
            }.call();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ElasticsearchTransportClientConnectionException(e.getMessage());
        }

        return transportClient;
    }

    private NodesStatsRequestBuilder getNodeStatsRequestBuilder() {
        return nodeStatsRequestBuilder;
    }

    public Client getTransportClient() {
        return client;
    }
}
