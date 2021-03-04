package com.netflix.raigad.utils;

import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.identity.RaigadInstance;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestElasticsearchUtils { @org.mockito.Mock

    @Mocked IConfiguration config;

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

    @Test
    public void TestAmIMasterNode() throws Exception {
        String expectedIp = "100.0.0.1";

        new Expectations() {
            {
                config.getHostIP();
                result = expectedIp;
                times = 1;

                config.getHostLocalIP();
                times = 0;
            }
        };

        new MockUp<SystemUtils>() {
            @Mock
            String runHttpGetCommand(String url) {
                return expectedIp;
            }
        };

        Assert.assertTrue(ElasticsearchUtils.amIMasterNode(config, new HttpModule(config)));
    }

    @Test
    public void TestAmIMasterNodeWithWhitespace() throws Exception {
        String expectedIp = "100.0.0.1";

        new Expectations() {
            {
                config.getHostIP();
                result = expectedIp;
                times = 1;

                config.getHostLocalIP();
                times = 0;
            }
        };

        new MockUp<SystemUtils>() {
            @Mock
            String runHttpGetCommand(String url) {
                return expectedIp + " \n ";
            }
        };

        Assert.assertTrue(ElasticsearchUtils.amIMasterNode(config, new HttpModule(config)));
    }

    @Test
    public void TestAmIMasterNodeExternalIp() throws Exception {
        String expectedLocalIp = "100.0.0.1";
        String expectedExternalIp = "54.0.0.1";

        new Expectations() {
            {
                config.getHostIP();
                result = expectedExternalIp;
                times = 1;

                config.getHostLocalIP();
                result = expectedLocalIp;
                times = 1;
            }
        };

        new MockUp<SystemUtils>() {
            @Mock
            String runHttpGetCommand(String url) {
                return expectedLocalIp;
            }
        };

        Assert.assertTrue(ElasticsearchUtils.amIMasterNode(config, new HttpModule(config)));
    }

    @Test
    public void TestAmIMasterNodeNegative() throws Exception {
        String expectedIp = "100.0.0.1";
        String returnedIp = "100.0.0.2";

        new Expectations() {
            {
                config.getHostIP();
                result = expectedIp;
                times = 1;

                config.getHostLocalIP();
                result = expectedIp;
                times = 1;
            }
        };

        new MockUp<SystemUtils>() {
            @Mock
            String runHttpGetCommand(String url) {
                return returnedIp;
            }
        };

        Assert.assertFalse(ElasticsearchUtils.amIMasterNode(config, new HttpModule(config)));
    }

    @Test
    public void TestAmIMasterNodeNegativeNull() throws Exception {
        new Expectations() {
            {
                config.getHostIP();
                times = 0;

                config.getHostLocalIP();
                times = 0;
            }
        };

        new MockUp<SystemUtils>() {
            @Mock
            String runHttpGetCommand(String url) {
                return null;
            }
        };

        Assert.assertFalse(ElasticsearchUtils.amIMasterNode(config, new HttpModule(config)));
    }

    @Test
    public void TestAmIMasterNodeNegativeEmpty() throws Exception {
        new Expectations() {
            {
                config.getHostIP();
                times = 0;

                config.getHostLocalIP();
                times = 0;
            }
        };

        new MockUp<SystemUtils>() {
            @Mock
            String runHttpGetCommand(String url) {
                return "";
            }
        };

        Assert.assertFalse(ElasticsearchUtils.amIMasterNode(config, new HttpModule(config)));
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
