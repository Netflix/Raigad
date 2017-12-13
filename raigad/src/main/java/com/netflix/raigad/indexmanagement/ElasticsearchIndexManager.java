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

package com.netflix.raigad.indexmanagement;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.indexmanagement.exception.UnsupportedAutoIndexException;
import com.netflix.raigad.scheduler.CronTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import com.netflix.raigad.utils.*;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Index retention will delete indices older than certain date e.g. if the current date is 10/28/2014,
 * retention period is 4, and given the following indices:
 * <p>
 * test_index20141024
 * test_index20141025
 * test_index20141026
 * test_index20141027
 * test_index20141028
 * <p>
 * Index to be deleted is test_index20141024.
 * <p>
 * If pre-create option is enabled, then one future index will be pre-created. Using the input data from above,
 * the following index will be pre-created: test_index20141029
 */
@Singleton
public class ElasticsearchIndexManager extends Task {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchIndexManager.class);

    public static String JOB_NAME = "ElasticsearchIndexManager";
    private final HttpModule httpModule;

    @Inject
    protected ElasticsearchIndexManager(IConfiguration config, HttpModule httpModule) {
        super(config);
        this.httpModule = httpModule;
    }

    Client getTransportClient() throws ElasticsearchTransportClientConnectionException {
        return ElasticsearchTransportClient.instance(config).getTransportClient();
    }

    @Override
    public void execute() {
        try {
            if (!config.isIndexAutoCreationEnabled()) {
                logger.info("Index management is disabled");
                return;
            }

            // Check is Elasticsearch is started
            if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
                logger.info("Elasticsearch is not yet started, skipping index management");
                return;
            }

            // Only active master can perform index management
            if (!ElasticsearchUtils.amIMasterNode(config, httpModule)) {
                if (config.isDebugEnabled()) {
                    logger.debug("Cannot perform index management: current node is not an active master node");
                }
                return;
            }

            runIndexManagement();
        } catch (Exception e) {
            logger.warn("Exception while performing index management", e);
        }
    }

    public void runIndexManagement() throws Exception {
        logger.info("Starting index management");

        String serializedIndexMetadata = config.getIndexMetadata();
        List<IndexMetadata> indexMetadataList;

        try {
            indexMetadataList = IndexUtils.parseIndexMetadata(serializedIndexMetadata);
        } catch (Exception e) {
            logger.error(String.format("Failed to build index metadata from %s", serializedIndexMetadata), e);
            return;
        }

        Client esTransportClient = getTransportClient();
        DateTime dateTime = new DateTime();

        runIndexManagement(esTransportClient, indexMetadataList, dateTime);
    }

    void runIndexManagement(Client esTransportClient, List<IndexMetadata> indexMetadataList, DateTime dateTime) {
        // Find all the indices
        IndicesStatsResponse indicesStatsResponse = getIndicesStatsResponse(esTransportClient);
        Map<String, IndexStats> indexStatsMap = indicesStatsResponse.getIndices();

        if (indexStatsMap == null || indexStatsMap.isEmpty()) {
            logger.info("Cluster is empty, no indices found");
            return;
        }

        for (IndexMetadata indexMetadata : indexMetadataList) {
            if (!indexMetadata.isActionable()) {
                logger.warn(String.format("Index metadata %s is not actionable, skipping", indexMetadata));
                continue;
            }

            try {
                checkIndexRetention(esTransportClient, indexStatsMap.keySet(), indexMetadata, dateTime);

                if (indexMetadata.isPreCreate()) {
                    preCreateIndex(esTransportClient, indexMetadata, dateTime);
                }
            } catch (Exception e) {
                logger.error("Caught an exception while building index metadata information from configuration property");
                return;
            }
        }
    }

    @Override
    public String getName() {
        return JOB_NAME;
    }

    public static TaskTimer getTimer(IConfiguration config) {
        return new CronTimer(config.getAutoCreateIndexScheduleMinutes(), 0, JOB_NAME);
    }

    void checkIndexRetention(Client esTransportClient, Set<String> indices, IndexMetadata indexMetadata, DateTime dateTime) throws UnsupportedAutoIndexException {
        // Calculate the past retention date
        int pastRetentionCutoffDateDate = IndexUtils.getPastRetentionCutoffDate(indexMetadata, dateTime);
        logger.info("Deleting indices that are older than {}", pastRetentionCutoffDateDate);

        indices.forEach(indexName -> {
            logger.info("Processing index [{}]", indexName);

            if (indexMetadata.getIndexNameFilter().filter(indexName) &&
                    indexMetadata.getIndexNameFilter().getNamePart(indexName).equalsIgnoreCase(indexMetadata.getIndexName())) {

                // Extract date from the index name
                try {
                    int indexDate = IndexUtils.getDateFromIndexName(indexMetadata, indexName);

                    if (indexDate < pastRetentionCutoffDateDate) {
                        logger.info("Date {} for index {} is past the retention date of {}, deleting it", indexDate, indexName, pastRetentionCutoffDateDate);
                        deleteIndices(esTransportClient, indexName, config.getAutoCreateIndexTimeout());
                    }
                } catch (UnsupportedAutoIndexException e) {
                    logger.error("Invalid index metadata: " + indexMetadata.toString(), e);
                }
            }
        });
    }

    void preCreateIndex(Client client, IndexMetadata indexMetadata, DateTime dateTime) throws UnsupportedAutoIndexException {
        logger.info("Pre-creating indices for {}*", indexMetadata.getIndexName());

        IndicesStatsResponse indicesStatsResponse = getIndicesStatsResponse(client);
        Map<String, IndexStats> indexStatsMap = indicesStatsResponse.getIndices();

        if (indexStatsMap == null || indexStatsMap.isEmpty()) {
            logger.info("No existing indices, no need to pre-create");
            return;
        }

        indexStatsMap.keySet().stream().filter(indexName -> indexMetadata.getIndexNameFilter().filter(indexName) &&
                indexMetadata.getIndexNameFilter().getNamePart(indexName).equalsIgnoreCase(indexMetadata.getIndexName()))
                .findFirst().ifPresent(indexName -> {
            try {
                createIndex(client, IndexUtils.getIndexNameToPreCreate(indexMetadata, dateTime));
            } catch (UnsupportedAutoIndexException e) {
                logger.error("Invalid index metadata: " + indexMetadata.toString(), e);
            }
        });
    }

    void createIndex(Client client, String indexName) {
        if (!client.admin().indices().prepareExists(indexName).execute().actionGet(config.getAutoCreateIndexTimeout()).isExists()) {
            client.admin().indices().prepareCreate(indexName).execute().actionGet(config.getAutoCreateIndexTimeout());
            logger.info(indexName + " has been created");
        } else {
            logger.warn(indexName + " already exists");
        }
    }

    void deleteIndices(Client client, String indexName, int timeout) {
        DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(indexName).execute().actionGet(timeout);

        if (deleteIndexResponse.isAcknowledged()) {
            logger.info(indexName + " deleted");
        } else {
            logger.warn("Failed to delete " + indexName);
            throw new RuntimeException("Failed to delete " + indexName);
        }
    }

    /**
     * Following method is isolated so that it helps in Unit Testing for Mocking
     *
     * @param esTransportClient
     * @return
     */
    IndicesStatsResponse getIndicesStatsResponse(Client esTransportClient) {
        return esTransportClient.admin().indices().prepareStats("_all").execute().actionGet(config.getAutoCreateIndexTimeout());
    }
}
