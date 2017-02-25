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
import com.netflix.raigad.objectmapper.DefaultIndexMapper;
import com.netflix.raigad.scheduler.CronTimer;
import com.netflix.raigad.scheduler.Task;
import com.netflix.raigad.scheduler.TaskTimer;
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import com.netflix.raigad.utils.ElasticsearchTransportClient;
import com.netflix.raigad.utils.ElasticsearchUtils;
import com.netflix.raigad.utils.HttpModule;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Index retention will get rid of (Retention Period in Days - 1) day indices for past days
 * e.g. Retention period = 5
 * Index Name = <test_index20141024>
 * Index Name = <test_index20141025>
 * Index Name = <test_index20141026>
 * Index Name = <test_index20141027>
 * Index Name = <test_index20141028>
 * <p>
 * Index to be deleted = test_index20141024
 * <p>
 * If Pre-Create is Enabled, it will create Today's Index + (Retention Period in Days - 1)day indices for future days
 * <p>
 * eg. for above example, following indices will be created with default settings:
 * <p>
 * Index Name = <test_index20141029>
 * Index Name = <test_index20141030>
 * Index Name = <test_index20141031>
 * Index Name = <test_index20141101>
 * Index Name = <test_index20141102>
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

    @Override
    public void execute() {
        try {
            //Confirm if Current Node is a Master Node
            if (ElasticsearchUtils.amIMasterNode(config, httpModule)) {
                // If Elasticsearch is started then only start Snapshot Backup
                if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
                    String exceptionMsg = "Elasticsearch is not yet started, not starting Index Management yet";
                    logger.info(exceptionMsg);
                    return;
                }

                logger.info("Current node is the master node");

                if (!config.isIndexAutoCreationEnabled()) {
                    logger.info("Autocreation of indices is disabled, moving on");
                    return;
                }

                runIndexManagement();
            } else {
                //TODO: Update config property
                if (config.isDebugEnabled()) {
                    logger.debug("Current node is not a master node yet, sleeping for " + config.getAutoCreateIndexPeriodicScheduledHour() + " seconds");
                }
            }
        } catch (Exception e) {
            logger.warn("Exception while performing index maintenance", e);
        }
    }

    public void runIndexManagement() throws Exception {
        logger.info("Starting index maintenance");

        String serializedIndexMetadata = config.getIndexMetadata();
        List<IndexMetadata> indexMetadataList;
        try {
            indexMetadataList = buildInfo(serializedIndexMetadata);
        } catch (Exception e) {
            //TODO: Add Servo monitoring so that it can be verified from dashboard
            logger.error("Failed to build index metadata information from configuration property: {}", serializedIndexMetadata);
            return;
        }

        Client esTransportClient = ElasticsearchTransportClient.instance(config).getTransportClient();

        for (IndexMetadata indexMetadata : indexMetadataList) {
            if (!indexMetadata.isActionable()) {
                continue;
            }

            if (esTransportClient == null) {
                continue;
            }

            try {
                checkIndexRetention(indexMetadata, esTransportClient);

                if (indexMetadata.isPreCreate()) {
                    preCreateIndex(indexMetadata, esTransportClient);
                }
            } catch (Exception e) {
                //TODO: Add Servo monitoring so that it can be verified from dashboard
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
        int hour = config.getAutoCreateIndexPeriodicScheduledHour();
        return new CronTimer(hour, 1, 0, JOB_NAME);
    }

    /**
     * Convert the JSON String of parameters to IndexMetadata objects
     *
     * @param serializedIndexMetadata : JSON string with parameters
     * @return list of IndexMetadata objects
     * @throws IOException
     */
    public static List<IndexMetadata> buildInfo(String serializedIndexMetadata) throws IOException {
        ObjectMapper jsonMapper = new DefaultIndexMapper();
        TypeReference<List<IndexMetadata>> typeRef = new TypeReference<List<IndexMetadata>>() {
        };
        return jsonMapper.readValue(serializedIndexMetadata, typeRef);
    }

    private void checkIndexRetention(IndexMetadata indexMetadata, Client esTransportClient) throws UnsupportedAutoIndexException {

        if (indexMetadata.getRetentionPeriod() == null) {
            logger.info("Retention period not set for Cluster is empty, no indices found");
            return;
        }

        // Calculate the past retention date
        int pastRetentionCutoffDateDate = IndexUtils.getPastRetentionCutoffDate(indexMetadata);
        logger.info("Deleting indices that are older than {}", pastRetentionCutoffDateDate);

        // Find all the indices
        IndicesStatsResponse indicesStatsResponse = getIndicesStatsResponse(esTransportClient);
        Map<String, IndexStats> indexStatsMap = indicesStatsResponse.getIndices();

        if (indexStatsMap == null || indexStatsMap.isEmpty()) {
            logger.info("Cluster is empty, no indices found");
            return;
        }

        for (String indexName : indexStatsMap.keySet()) {
            logger.info("Processing index [{}]", indexName);

            if (indexMetadata.getIndexNameFilter().filter(indexName) &&
                    indexMetadata.getIndexNameFilter().getNamePart(indexName).equalsIgnoreCase(indexMetadata.getIndexName())) {

                //Extract date from the index name
                int indexDate = IndexUtils.getDateFromIndexName(indexMetadata, indexName);
                logger.info("Extracted date {} from index {}", indexDate, indexName);

                //Delete old indices
                if (indexDate <= pastRetentionCutoffDateDate) {
                    logger.info("Date {} for index {} is past the retention date of {}, deleting this index now",
                            indexDate, indexName, pastRetentionCutoffDateDate);
                    deleteIndices(esTransportClient, indexName, config.getAutoCreateIndexTimeout());
                }
            }
        }
    }

    private void deleteIndices(Client client, String indexName, int timeout) {
        logger.info("Attempting to delete {} with timeout of {} ms", indexName, timeout);
        DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(indexName).execute().actionGet(timeout);

        if (!deleteIndexResponse.isAcknowledged()) {
            throw new RuntimeException("Failed to delete " + indexName);
        } else {
            logger.info(indexName + " deleted");
        }
    }

    private void preCreateIndex(IndexMetadata indexMetadata, Client esTransportClient) throws UnsupportedAutoIndexException {
        logger.info("Pre-creating indices");

        IndicesStatsResponse indicesStatsResponse = getIndicesStatsResponse(esTransportClient);
        Map<String, IndexStats> indexStatsMap = indicesStatsResponse.getIndices();

        if (indexStatsMap == null || indexStatsMap.isEmpty()) {
            logger.info("No existing indices, no need to pre-create");
            return;
        }

        for (String indexNameWithDateSuffix : indexStatsMap.keySet()) {
            logger.debug("Processing pre-creation for index {}", indexNameWithDateSuffix);

            if (indexMetadata.getIndexNameFilter().filter(indexNameWithDateSuffix) &&
                    indexMetadata.getIndexNameFilter().getNamePart(indexNameWithDateSuffix).equalsIgnoreCase(indexMetadata.getIndexName())) {

                for (int i = 0; i < indexMetadata.getRetentionPeriod(); ++i) {
                    DateTime dateTime = new DateTime();
                    int addedDate;

                    switch (indexMetadata.getRetentionType()) {
                        case DAILY:
                            dateTime = dateTime.plusDays(i);
                            addedDate = Integer.parseInt(String.format("%d%02d%02d", dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth()));
                            break;

                        case MONTHLY:
                            dateTime = dateTime.plusMonths(i);
                            addedDate = Integer.parseInt(String.format("%d%02d", dateTime.getYear(), dateTime.getMonthOfYear()));
                            break;

                        case YEARLY:
                            dateTime = dateTime.plusYears(i);
                            addedDate = Integer.parseInt(String.format("%d", dateTime.getYear()));
                            break;

                        default:
                            throw new UnsupportedAutoIndexException("Given index is not (DAILY or MONTHLY or YEARLY), please check your configuration");
                    }

                    if (config.isDebugEnabled()) {
                        logger.debug("Appended date [{}]", addedDate);
                    }

                    String newIndexName = indexMetadata.getIndexName() + addedDate;

                    logger.info("Pre-creating index [{}]", newIndexName);

                    if (!esTransportClient.admin().indices().prepareExists(newIndexName).execute().actionGet(config.getAutoCreateIndexTimeout()).isExists()) {
                        esTransportClient.admin().indices().prepareCreate(newIndexName).execute().actionGet(config.getAutoCreateIndexTimeout());
                        logger.info(newIndexName + " has been created");
                    } else {
                        //TODO: Change to debug after testing
                        logger.warn(newIndexName + " already exists");
                    }
                }
            }
        }
    }

    /**
     * Following method is isolated so that it helps in Unit Testing for Mocking
     *
     * @param esTransportClient
     * @return
     */
    private IndicesStatsResponse getIndicesStatsResponse(Client esTransportClient) {
        return esTransportClient.admin().indices().prepareStats("_all").execute().actionGet(config.getAutoCreateIndexTimeout());
    }

}
