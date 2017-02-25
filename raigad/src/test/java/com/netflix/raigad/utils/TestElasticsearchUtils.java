package com.netflix.raigad.utils;

import com.netflix.raigad.identity.RaigadInstance;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestElasticsearchUtils {

    @Test
    public void TestInstaceToJson() {
        System.out.println("Starting a test ...");
        List<RaigadInstance> instances = getRaigadInstances();
        JSONObject jsonInstances = ElasticsearchUtils.transformRaigadInstanceToJson(instances);
        System.out.println(jsonInstances);
        List<RaigadInstance> returnedInstances = ElasticsearchUtils.getRaigadInstancesFromJson(jsonInstances);
        System.out.println("Num of Returned instances = " + returnedInstances.size());

        for (RaigadInstance esInst : returnedInstances)
            System.out.println("-->" + esInst);
    }

    public static List<RaigadInstance> getRaigadInstances() {
        List<RaigadInstance> instances = new ArrayList<RaigadInstance>();
        for (int i = 0; i < 3; i++) {
            RaigadInstance inst = new RaigadInstance();
            inst.setApp("cluster-" + i);
            inst.setAvailabilityZone("1d");
            inst.setDC("us-east1");
            inst.setHostIP("127.0.0." + i);
            inst.setHostName("host-" + i);
            inst.setId("id-" + i);
            inst.setInstanceId("instance-" + i);
            inst.setUpdatetime(12345567);
            instances.add(inst);
        }
        return instances;
    }
}
