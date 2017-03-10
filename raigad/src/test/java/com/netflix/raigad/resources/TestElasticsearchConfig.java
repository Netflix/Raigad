package com.netflix.raigad.resources;

import com.google.common.collect.ImmutableList;
import com.netflix.raigad.identity.InstanceManager;
import com.netflix.raigad.identity.RaigadInstance;
import com.netflix.raigad.startup.RaigadServer;
import com.netflix.raigad.utils.TribeUtils;
import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.core.Response;
import java.net.UnknownHostException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(JMockit.class)
public class TestElasticsearchConfig {
    @Test
    public void getNodes(
            @Mocked final RaigadServer raigadServer,
            @Mocked final TribeUtils tribeUtils,
            @Mocked final InstanceManager instanceManager) throws Exception {
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(raigadServer, tribeUtils);

        RaigadInstance raigadInstance1 = new RaigadInstance();
        raigadInstance1.setApp("fake-app1");

        RaigadInstance raigadInstance2 = new RaigadInstance();
        raigadInstance2.setApp("fake-app2");

        RaigadInstance raigadInstance3 = new RaigadInstance();
        raigadInstance3.setApp("fake-app3");

        final List<RaigadInstance> nodes = asList(raigadInstance1, raigadInstance2, raigadInstance3);

        new Expectations() {
            {
                raigadServer.getInstanceManager();
                result = instanceManager;
                times = 1;

                instanceManager.getAllInstances();
                result = nodes;
                times = 1;
            }
        };

        Response response = elasticsearchConfig.getNodes();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void getNodes_notFound(
            @Mocked final RaigadServer raigadServer,
            @Mocked final TribeUtils tribeUtils,
            @Mocked final InstanceManager instanceManager) throws Exception {
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(raigadServer, tribeUtils);
        final List<String> nodes = ImmutableList.of();

        new Expectations() {
            {
                raigadServer.getInstanceManager();
                result = instanceManager;
                times = 1;

                instanceManager.getAllInstances();
                result = nodes;
                times = 1;
            }
        };

        Response response = elasticsearchConfig.getNodes();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void getNodes_Error(
            @Mocked final RaigadServer raigadServer,
            @Mocked final TribeUtils tribeUtils,
            @Mocked final InstanceManager instanceManager) throws Exception {
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(raigadServer, tribeUtils);
        final List<String> nodes = null;

        new Expectations() {
            {
                raigadServer.getInstanceManager();
                result = instanceManager;
                times = 1;

                instanceManager.getAllInstances();
                result = nodes;
                times = 1;
            }
        };

        Response response = elasticsearchConfig.getNodes();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void getNodes_handlesUnknownHostException(
            @Mocked final RaigadServer raigadServer,
            @Mocked final TribeUtils tribeUtils,
            @Mocked final InstanceManager instanceManager) throws Exception {
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(raigadServer, tribeUtils);

        new Expectations() {
            {
                raigadServer.getInstanceManager();
                result = instanceManager;
                times = 1;

                instanceManager.getAllInstances();
                result = new UnknownHostException();
            }
        };

        Response response = elasticsearchConfig.getNodes();
        assertEquals(500, response.getStatus());
    }
}
