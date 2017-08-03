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

import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.identity.RaigadInstance;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchUtils {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchUtils.class);

    private static final String HOST_NAME = "host_name";
    private static final String ID = "id";
    private static final String APP_NAME = "app_name";
    private static final String INSTANCE_ID = "instance_id";
    private static final String AVAILABILITY_ZONE = "availability_zone";
    private static final String PUBLIC_IP = "public_ip";
    private static final String DC = "dc";
    private static final String UPDATE_TIME = "update_time";
    private static final String HTTP_TAG = "http://";
    private static final String URL_PORT_SEPARATOR = ":";
    private static final String ELASTICSEARCH_HTTP_PORT = "7104";
    private static final String URL_PATH_SEPARATOR = "/";
    private static final String URL_QUERY_SEPARATOR = "?";
    private static final String REPOSITORY_VERIFICATION_PARAM = "_snapshot";
    private static final String SNAPSHOT_COMPLETION_PARAM = "wait_for_completion=true";
    private static final String DEFAULT_SNAPSHOT_IGNORE_AVAILABLE_PARAM = "true";
    private static final char PATH_SEP = File.separatorChar;
    private static final String S3_REPO_DATE_FORMAT = "yyyyMMdd";
    private static final DateTimeZone currentZone = DateTimeZone.UTC;

    @SuppressWarnings("unchecked")
    public static JSONObject transformRaigadInstanceToJson(List<RaigadInstance> instances) {
        JSONObject esJsonInstances = new JSONObject();

        for (int i = 0; i < instances.size(); i++) {
            JSONObject jsInstance = new JSONObject();
            jsInstance.put(HOST_NAME, instances.get(i).getHostName());
            jsInstance.put(ID, instances.get(i).getId());
            jsInstance.put(APP_NAME, instances.get(i).getApp());
            jsInstance.put(INSTANCE_ID, instances.get(i).getInstanceId());
            jsInstance.put(AVAILABILITY_ZONE, instances.get(i).getAvailabilityZone());
            jsInstance.put(PUBLIC_IP, instances.get(i).getHostIP());
            jsInstance.put(DC, instances.get(i).getDC());
            jsInstance.put(UPDATE_TIME, instances.get(i).getUpdatetime());

            JSONArray esJsonInstance = new JSONArray();
            esJsonInstance.add(jsInstance);

            esJsonInstances.put("instance-" + i, jsInstance);
        }

        JSONObject allInstances = new JSONObject();
        allInstances.put("instances", esJsonInstances);
        return allInstances;
    }

    public static List<RaigadInstance> getRaigadInstancesFromJson(JSONObject instances) {
        List<RaigadInstance> raigadInstances = new ArrayList<>();

        JSONObject topLevelInstance = (JSONObject) instances.get("instances");

        for (int i = 0; ; i++) {
            if (topLevelInstance.get("instance-" + i) == null) {
                break;
            }

            JSONObject eachInstance = (JSONObject) topLevelInstance.get("instance-" + i);

            // Build RaigadInstance
            RaigadInstance raigadInstance = new RaigadInstance();
            raigadInstance.setApp((String) eachInstance.get(APP_NAME));
            raigadInstance.setAvailabilityZone((String) eachInstance.get(AVAILABILITY_ZONE));
            raigadInstance.setDC((String) eachInstance.get(DC));
            raigadInstance.setHostIP((String) eachInstance.get(PUBLIC_IP));
            raigadInstance.setHostName((String) eachInstance.get(HOST_NAME));
            raigadInstance.setId((String) eachInstance.get(ID));
            raigadInstance.setInstanceId((String) eachInstance.get(INSTANCE_ID));
            raigadInstance.setUpdatetime((Long) eachInstance.get(UPDATE_TIME));

            // Add to the list
            raigadInstances.add(raigadInstance);
        }

        return raigadInstances;
    }

    public static boolean amIMasterNode(IConfiguration config, HttpModule httpModule) throws Exception {
        String URL = httpModule.findMasterNodeURL();
        String response = SystemUtils.runHttpGetCommand(URL);

        if (config.isDebugEnabled()) {
            logger.debug("Calling {} returned: {}", URL, response);
        }

        response = StringUtils.trim(response);

        // Check the response
        if (StringUtils.isEmpty(response)) {
            logger.error("Response from " + URL + " is empty");
            return false;
        }

        // Checking if the current node is a master node
        if (response.equalsIgnoreCase(config.getHostIP()) || response.equalsIgnoreCase(config.getHostLocalIP())) {
            return true;
        }

        return false;
    }

    public static List<String> getAvailableSnapshots(Client transportClient, String repositoryName) {
        logger.info("Searching for available snapshots");

        List<String> snapshots = new ArrayList<>();
        GetSnapshotsResponse getSnapshotsResponse = transportClient.admin().cluster()
                .prepareGetSnapshots(repositoryName)
                .get();

        for (SnapshotInfo snapshotInfo : getSnapshotsResponse.getSnapshots()) {
            snapshots.add(snapshotInfo.snapshotId().getName());
        }

        return snapshots;
    }

    /**
     * Repository Name is Today's Date in yyyyMMdd format eg. 20140630
     *
     * @return Repository Name
     */
    public static String getS3RepositoryName() {
        DateTime dateTime = new DateTime();
        DateTime dateTimeGmt = dateTime.withZone(currentZone);
        return formatDate(dateTimeGmt, S3_REPO_DATE_FORMAT);
    }

    public static String formatDate(DateTime dateTime, String dateFormat) {
        DateTimeFormatter fmt = DateTimeFormat.forPattern(dateFormat);
        return dateTime.toString(fmt);
    }
}
