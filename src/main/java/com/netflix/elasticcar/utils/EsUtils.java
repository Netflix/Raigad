package com.netflix.elasticcar.utils;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.elasticcar.identity.ElasticCarInstance;

public class EsUtils 
{
    private static final Logger logger = LoggerFactory.getLogger(EsUtils.class);
    private static final String HOST_NAME = "host_name";
    private static final String ID = "id";
    private static final String APP_NAME = "app_name";
    private static final String INSTANCE_ID = "instance_id";
    private static final String AVAILABILITY_ZONE = "availability_zone";
    private static final String PUBLIC_IP = "public_ip";
    private static final String DC = "dc";
    private static final String UPDATE_TIME = "update_time";

    
    @SuppressWarnings("unchecked")
	public static JSONObject transformEsCarInstanceToJson(List<ElasticCarInstance> instances)
    {
    		JSONObject esJsonInstances = new JSONObject();
    		
    		for(int i=0;i<instances.size();i++)
    		{
    	   		JSONArray esJsonInstance = new JSONArray();
    	   		
    	   	 	JSONObject jsInstance = new JSONObject();
    			jsInstance.put(HOST_NAME, instances.get(i).getHostName());
    			jsInstance.put(ID, instances.get(i).getId());
    			jsInstance.put(APP_NAME, instances.get(i).getApp());
    			jsInstance.put(INSTANCE_ID, instances.get(i).getInstanceId());
    			jsInstance.put(AVAILABILITY_ZONE, instances.get(i).getAvailabilityZone());
    			jsInstance.put(PUBLIC_IP, instances.get(i).getHostIP());
    			jsInstance.put(DC, instances.get(i).getDC());
    			jsInstance.put(UPDATE_TIME, instances.get(i).getUpdatetime());
    			esJsonInstance.add(jsInstance);
    			esJsonInstances.put("instance-"+i,jsInstance);
    		}    	
    		
    		JSONObject allInstances = new JSONObject();
    		allInstances.put("instances", esJsonInstances);
    		return allInstances;
    }
    
	public static List<ElasticCarInstance> getEsCarInstancesFromJson(JSONObject instances)
    {
		List<ElasticCarInstance> esCarInstances = new ArrayList<ElasticCarInstance>();
		
		JSONObject topLevelInstance = (JSONObject) instances.get("instances");
		
		for(int i=0;;i++)
		{
			if(topLevelInstance.get("instance-"+i) == null)
				break;
			JSONObject eachInstance = (JSONObject) topLevelInstance.get("instance-"+i);
			//Build ElasticCarInstance
			ElasticCarInstance escInstance = new ElasticCarInstance();
			escInstance.setApp((String) eachInstance.get(APP_NAME));
			escInstance.setAvailabilityZone((String) eachInstance.get(AVAILABILITY_ZONE));
			escInstance.setDC((String) eachInstance.get(DC));
			escInstance.setHostIP((String) eachInstance.get(PUBLIC_IP));
			escInstance.setHostName((String) eachInstance.get(HOST_NAME));
			escInstance.setId((String) eachInstance.get(ID));
			escInstance.setInstanceId((String) eachInstance.get(INSTANCE_ID));
			escInstance.setUpdatetime((Long) eachInstance.get(UPDATE_TIME));
			//Add to the list
			esCarInstances.add(escInstance);
		}
  		
    		return esCarInstances;
    }
}
