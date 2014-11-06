/**
 * Copyright 2014 Netflix, Inc.
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
package org.elasticsearch.discovery.custom;

import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticsearchUtil 
{
    private static final String TOP_LEVEL_ELEMENT = "instances";
	private static final String HOST_NAME = "host_name";
    private static final String ID = "id";
    private static final String APP_NAME = "app_name";
    private static final String INSTANCE_ID = "instance_id";
    private static final String AVAILABILITY_ZONE = "availability_zone";
    private static final String PUBLIC_IP = "public_ip";
    private static final String DC = "dc";
    private static final String UPDATE_TIME = "update_time";
    

	@SuppressWarnings("unchecked")
	public static List<ElasticCarInstance> getEsCarInstancesFromJsonString(String jsonInstances,ESLogger logger)
    {
		List<ElasticCarInstance> esCarInstances = new ArrayList<ElasticCarInstance>();
		
        try {
			JsonPath jsonPath = new JsonPath(jsonInstances);
			Map<String,Object> topLevelInstanceMap = (Map<String, Object>) jsonPath.jsonMap.get(TOP_LEVEL_ELEMENT);
			for(String instanceKey : topLevelInstanceMap.keySet())
			{
				Map<String,Object> instParamMap = (Map<String, Object>) topLevelInstanceMap.get(instanceKey);
				ElasticCarInstance escInstance = new ElasticCarInstance();
				escInstance.setApp((String) instParamMap.get(APP_NAME));
				escInstance.setAvailabilityZone((String) instParamMap.get(AVAILABILITY_ZONE));
				escInstance.setDC((String) instParamMap.get(DC));
				escInstance.setHostIP((String) instParamMap.get(PUBLIC_IP));
				escInstance.setHostName((String) instParamMap.get(HOST_NAME));
				escInstance.setId((String) instParamMap.get(ID));
				escInstance.setInstanceId((String) instParamMap.get(INSTANCE_ID));
				escInstance.setUpdatetime((Long) instParamMap.get(UPDATE_TIME));
				logger.info("EsInstance = ("+escInstance.toString()+")");
				//Add to the list
				esCarInstances.add(escInstance);
			}
			
		} catch (IOException e) {
			logger.error(" Error caught during Json Parsing", e);
		}

        return esCarInstances;
    }
}
