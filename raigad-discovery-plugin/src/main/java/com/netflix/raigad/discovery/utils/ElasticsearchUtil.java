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

package com.netflix.raigad.discovery.utils;

import com.netflix.raigad.discovery.RaigadInstance;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticsearchUtil {
    private static final String TOP_LEVEL_ELEMENT = "instances";

    private static final String ID = "id";
    private static final String APP_NAME = "app_name";
    private static final String HOST_NAME = "host_name";
    private static final String INSTANCE_ID = "instance_id";
    private static final String AVAILABILITY_ZONE = "availability_zone";
    private static final String PUBLIC_IP = "public_ip";
    private static final String DC = "dc";
    private static final String UPDATE_TIME = "update_time";

    @SuppressWarnings("unchecked")
    public static List<RaigadInstance> getRaigadInstancesFromJsonString(String jsonInstances, Logger logger) {
        List<RaigadInstance> raigadInstances = new ArrayList<RaigadInstance>();

        try {
            Map<String, Object> topLevelInstanceMap = (Map<String, Object>) jsonToMap(jsonInstances).get(TOP_LEVEL_ELEMENT);

            for (String instanceKey : topLevelInstanceMap.keySet()) {
                Map<String, Object> instParamMap = (Map<String, Object>) topLevelInstanceMap.get(instanceKey);
                RaigadInstance raigadInstance = new RaigadInstance();
                raigadInstance.setApp((String) instParamMap.get(APP_NAME));
                raigadInstance.setAvailabilityZone((String) instParamMap.get(AVAILABILITY_ZONE));
                raigadInstance.setDC((String) instParamMap.get(DC));
                raigadInstance.setHostIP((String) instParamMap.get(PUBLIC_IP));
                raigadInstance.setHostName((String) instParamMap.get(HOST_NAME));
                raigadInstance.setId((String) instParamMap.get(ID));
                raigadInstance.setInstanceId((String) instParamMap.get(INSTANCE_ID));
                raigadInstance.setUpdatetime((Long) instParamMap.get(UPDATE_TIME));
                logger.info("Raigad instance: {}", raigadInstance.toString());

                //Add to the list
                raigadInstances.add(raigadInstance);
            }
        } catch (IOException e) {
            logger.error("Error caught while parsing JSON", e);
        }

        return raigadInstances;
    }

    private static Map<String, Object> jsonToMap(String jsonString) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, jsonString)) {
            return parser.mapOrdered();
        }
    }
}
