package com.netflix.elasticcar.utils;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.junit.Test;

import com.netflix.elasticcar.identity.ElasticCarInstance;

public class TestEsUtils {

	@Test
	public void TestInstaceToJson()
	{
		System.out.println("Starting a test ...");
		List<ElasticCarInstance> instances = getEsCarInstances();
		JSONObject jsonInstances = EsUtils.transformEsCarInstanceToJson(instances);
		System.out.println(jsonInstances);
		List<ElasticCarInstance> returnedInstances = EsUtils.getEsCarInstancesFromJson(jsonInstances);
		System.out.println("Num of Returned instances = "+returnedInstances.size());
		
		for(ElasticCarInstance esInst : returnedInstances)
			System.out.println("-->"+esInst);
	}
	
	
	public static List<ElasticCarInstance> getEsCarInstances()
	{
		List<ElasticCarInstance> instances = new ArrayList<ElasticCarInstance>();		
		for(int i=0; i<3;i++)
		{
			ElasticCarInstance inst = new ElasticCarInstance();
			inst.setApp("cluster-"+i);
			inst.setAvailabilityZone("1d");
			inst.setDC("us-east1");
			inst.setHostIP("127.0.0."+i);
			inst.setHostName("host-"+i);
			inst.setId("id-"+i);
			inst.setInstanceId("instance-"+i);
			inst.setUpdatetime(12345567);			
			instances.add(inst);
		}		
		return instances;		
	}
}
