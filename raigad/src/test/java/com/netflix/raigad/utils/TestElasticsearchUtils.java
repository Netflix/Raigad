package com.netflix.raigad.utils;

import com.netflix.raigad.identity.RaigadInstance;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestElasticsearchUtils {

    @Test
    public void TestInstanceToJson() {
        System.out.println("Starting a test...");
        List<RaigadInstance> instances = getRaigadInstances();
        JSONObject jsonInstances = ElasticsearchUtils.transformRaigadInstanceToJson(instances);
        System.out.println(jsonInstances);
        List<RaigadInstance> returnedInstances = ElasticsearchUtils.getRaigadInstancesFromJson(jsonInstances);
        System.out.println("Number of returned instances = " + returnedInstances.size());

        for (RaigadInstance raigadInstance : returnedInstances) {
            System.out.println("-->" + raigadInstance);
        }
    }

    public static List<RaigadInstance> getRaigadInstances() {
        List<RaigadInstance> instances = new ArrayList<RaigadInstance>();

        for (int i = 0; i < 3; i++) {
            RaigadInstance raigadInstance = new RaigadInstance();
            raigadInstance.setApp("cluster-" + i);
            raigadInstance.setAvailabilityZone("1d");
            raigadInstance.setDC("us-east1");
            raigadInstance.setHostIP("127.0.0." + i);
            raigadInstance.setHostName("host-" + i);
            raigadInstance.setId("id-" + i);
            raigadInstance.setInstanceId("instance-" + i);
            raigadInstance.setUpdatetime(12345567);

            instances.add(raigadInstance);
        }

        return instances;
    }
}
